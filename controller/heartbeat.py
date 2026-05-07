"""Health monitoring and primary election for storage nodes."""

import grpc
import threading
import time
import logging
import sys
import docker
import os
from typing import Optional, Dict
from docker.errors import DockerException

# immediatly flush any logging to docker
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s:heartbeat.%(funcName)s:%(message)s',
    stream=sys.stdout,
    force=True
)
logger = logging.getLogger(__name__)

try:
    from gRPC import mktplace_pb2
    from gRPC import mktplace_pb2_grpc
    from gRPC import service_node_pb2
    from gRPC import service_node_pb2_grpc
except ImportError:
    logger.warning("Proto files not found. See README.md for instructions on generating gRPC code.")

from .registries import StorageNodeRegistry, ServiceNodeRegistry


class HealthMonitor(threading.Thread):
    """Background thread that monitors storage node health via heartbeats and manages primary election."""

    def __init__(self, storage_registry: StorageNodeRegistry, 
                 service_registry: ServiceNodeRegistry, 
                 service_node_addresses: Dict[int, str] = None,
                 heartbeat_interval: int = 5):
        super().__init__(daemon=True)
        self.storage_registry = storage_registry
        self.service_registry = service_registry
        self.service_node_addresses = service_node_addresses or {}
        self.heartbeat_interval = heartbeat_interval
        self.running = True
        
        # connect to docker
        try:
            self.docker_client = docker.from_env()
            logger.info("[HealthMonitor] Connected to Docker daemon")
        except DockerException as e:
            logger.error(f"[HealthMonitor] Failed to connect to Docker: {e}")
            self.docker_client = None
        
        # Track which nodes have already been recovered to avoid duplicate failovers
        self.failed_nodes_handled = set()
    
    def run(self):
        """Periodically check health of all nodes."""
        logger.info("[Controller] Waiting 30 seconds for data replication to backups...")
        time.sleep(30)  # Wait for primary to replicate data to backups before starting heartbeats
        while self.running:
            time.sleep(self.heartbeat_interval)
            self._send_heartbeats()
            self._check_primary_health()
    
    def _send_heartbeats(self):
        """Send heartbeat to all storage nodes via simple GetItem call."""
        nodes = self.storage_registry.get_all_nodes()
        
        for node_id, node_info in nodes.items():
            address = node_info['address']
            try:
                with grpc.insecure_channel(address) as channel:
                    stub = mktplace_pb2_grpc.MarketplaceServiceStub(channel)
                    # Use getitem as a simple heartbeat check
                    response = stub.GetItem(
                        mktplace_pb2.GetItemRequest(item_id="__heartbeat__"),
                        timeout=2
                    )
                    self.storage_registry.mark_healthy(node_id, True)
                    logger.info(f"[HealthMonitor] Heartbeat to storage node {node_id} successful")
            except Exception as e:
                logger.error(f"[HealthMonitor] Heartbeat to storage node {node_id} failed: {e}")
                self.storage_registry.mark_healthy(node_id, False)
                
                # trigger failover
                if node_id not in self.failed_nodes_handled:
                    logger.warning(f"[HealthMonitor] Storage node {node_id} is down! Initiating failover...")
                    self.failed_nodes_handled.add(node_id)
                    self._run_failover(node_id)
    
    def _check_primary_health(self):
        """Check if primary is still healthy. If not, elect a new primary."""
        primary_id = self.storage_registry.get_primary()
        if primary_id is None:
            # No primary yet, elect one
            self._elect_primary()
            return
        
        # Primary exists, check if it's healthy
        nodes = self.storage_registry.get_all_nodes()
        if primary_id in nodes and nodes[primary_id]['healthy']:
            # Primary is healthy
            return
        
        # Primary is unhealthy, elect new one
        logger.warning(f"Primary node {primary_id} is unhealthy. Electing new primary...")
        self._elect_primary()
    
    def _elect_primary(self):
        """Elect a new primary from healthy nodes."""
        new_primary = self.storage_registry.elect_new_primary()
        
        if new_primary is None:
            logger.error("No healthy nodes available for primary election!")
            return
        
        logger.info(f"Elected node {new_primary} as new primary")
    
    def _run_failover(self, failed_storage_id: int):
        """
        Recover steps for storage node failure:
        1. Mark failed node as unhealthy
        2. Elect new primary (if failed node was primary)
        3. Create new Docker container to replace failed node
        4. Replicate all data from primary to new node
        5. Mark affected service nodes as healthy
        6. Update service node assignments
        """
        logger.info(f"[HealthMonitor] Starting failover for storage node {failed_storage_id}")
        
        # step 1: Mark failed node as unhealthy
        logger.info(f"[HealthMonitor] Step 1: Marking storage node {failed_storage_id} as unhealthy")
        self.storage_registry.mark_healthy(failed_storage_id, False)
        
        # step 2: If failed node was primary, elect new primary
        primary_id = self.storage_registry.get_primary()
        if primary_id == failed_storage_id:
            logger.info(f"[HealthMonitor] Step 2: Failed node was primary. Electing new primary...")
            new_primary = self.storage_registry.elect_new_primary()
            if new_primary is not None:
                logger.info(f"[HealthMonitor] Step 2: Elected node {new_primary} as new primary")
            else:
                logger.error(f"[HealthMonitor] Step 2: Could not elect new primary - no healthy nodes available")
                return
        else:
            logger.info(f"[HealthMonitor] Step 2: Failed node was not primary (primary is {primary_id})")
        
        # step 3: create new Docker container to replace the failed node
        if self.docker_client:
            logger.info(f"[HealthMonitor] Step 3: Creating new Docker container to replace node {failed_storage_id}")
            if not self._create_replacement_container(failed_storage_id):
                logger.error(f"[HealthMonitor] Step 3: Failed to create replacement container")
                return
        else:
            logger.error(f"[HealthMonitor] Step 3: Docker client not available, cannot create container")
            return
        
        # Wait a bit for the new container to start and be ready
        logger.info(f"[HealthMonitor] Waiting for new container to be ready...")
        time.sleep(5)
        
        # step 4: Replicate all data from primary to new node
        logger.info(f"[HealthMonitor] Step 4: Replicating all data from primary to new node {failed_storage_id}")
        primary_id = self.storage_registry.get_primary()
        if not self._replicate_data_to_new_node(primary_id, failed_storage_id):
            logger.error(f"[HealthMonitor] Step 4: Failed to replicate data")
            return
        
        # step 5 mark the recovered node as healthy now that it has data
        logger.info(f"[HealthMonitor] Marking recovered node {failed_storage_id} as healthy")
        self.storage_registry.mark_healthy(failed_storage_id, True)
        
        # Step 5: Get affected service nodes and mark them as healthy again
        affected_services = self.service_registry.get_services_on_storage(failed_storage_id)
        if affected_services:
            logger.info(f"[HealthMonitor] Step 5: Marking {len(affected_services)} affected service nodes as healthy")
            for service_id in affected_services:
                self.service_registry.mark_healthy(service_id, True)
                logger.info(f"[HealthMonitor] Step 5: Marked service node {service_id} as healthy")
        else:
            logger.info(f"[HealthMonitor] Step 5: No affected service nodes")
        
        # Step 6: Update service node assignments to point to recovered storage node
        logger.info(f"[HealthMonitor] Step 6: Updating service node assignments")
        for service_id in affected_services:
            service_address = self.service_node_addresses.get(service_id)
            if not service_address:
                logger.error(f"[HealthMonitor] Step 6: Could not find address for service node {service_id}")
                continue
            
            recovered_node_address = self.storage_registry.get_node_address(failed_storage_id)
            if not recovered_node_address:
                logger.error(f"[HealthMonitor] Step 6: Could not find address for recovered storage node {failed_storage_id}")
                continue
            
            try:
                channel = grpc.insecure_channel(service_address)
                stub = service_node_pb2_grpc.ServiceNodeControlStub(channel)
                
                request = service_node_pb2.UpdateStorageNodeRequest(
                    service_node_id=service_id,
                    storage_node_address=recovered_node_address,
                    storage_node_id=failed_storage_id
                )
                response = stub.UpdateStorageNode(request, timeout=10)
                
                if response.success:
                    logger.info(f"[HealthMonitor] Step 6: Updated service node {service_id} to recovered storage {failed_storage_id}")
                else:
                    logger.error(f"[HealthMonitor] Step 6: Failed to update service node {service_id}")
                
                channel.close()
            except Exception as e:
                logger.error(f"[HealthMonitor] Step 6: Error updating service node {service_id}: {e}")
        
        logger.info(f"[HealthMonitor] Failover completed for storage node {failed_storage_id}")
    
    def _create_replacement_container(self, failed_node_id: int) -> bool:
        """
        Create a new Docker container to replace the failed storage node.
        
        Returns True if successful, False otherwise.
        """
        if not self.docker_client:
            logger.error(f"[HealthMonitor] Docker client not available")
            return False
        
        container_name = f'storage-node-{failed_node_id}'
        
        try:
            # First, remove the old container if it exists
            try:
                old_container = self.docker_client.containers.get(container_name)
                logger.info(f"[HealthMonitor] Removing old container {container_name}")
                old_container.remove(force=True)
                time.sleep(1)  # Wait a moment after removal
            except docker.errors.NotFound:
                logger.info(f"[HealthMonitor] Old container {container_name} not found (expected if it crashed)")
            except Exception as e:
                logger.error(f"[HealthMonitor] Error removing old container: {e}")
            
            # Get the marketplace network
            networks = self.docker_client.networks.list(
                filters={'name': 'marketplace-network'}
            )
            network = networks[0] if networks else None
            
            if not network:
                logger.error(f"[HealthMonitor] Could not find marketplace-network")
                return False
            
            # Find the storage node image
            images = self.docker_client.images.list(filters={'reference': '*storage-node*'})
            if not images:
                logger.error(f"[HealthMonitor] Could not find storage node image")
                return False
            
            image = images[0]
            logger.info(f"[HealthMonitor] Using image: {image.tags}")
            
            # Create new container
            logger.info(f"[HealthMonitor] Creating new container: {container_name}")
            container = self.docker_client.containers.run(
                image.id,
                detach=True,
                name=container_name,
                environment=[
                    f'NODE_ID={failed_node_id}',
                    'GRPC_HOST=0.0.0.0',
                    'GRPC_PORT=50051'
                ],
                network=network.name,
                healthcheck={
                    'test': ['CMD', 'python', '-c', 'import grpc; grpc.insecure_channel("localhost:50051")'],
                    'interval': 5000000000,  # 5 seconds in nanoseconds
                    'timeout': 2000000000,   # 2 seconds in nanoseconds
                    'retries': 2,
                    'start_period': 10000000000  # 10 seconds in nanoseconds
                }
            )
            
            logger.info(f"[HealthMonitor] Created container {container_name} with ID {container.id[:12]}")
            return True
            
        except Exception as e:
            logger.error(f"[HealthMonitor] Error creating replacement container: {e}", exc_info=True)
            return False
    
    def _replicate_data_to_new_node(self, primary_id: int, target_node_id: int) -> bool:
        """
        Replicate all data from primary node to the new replacement node.
        
        Returns True if successful, False otherwise.
        """
        primary_address = self.storage_registry.get_node_address(primary_id)
        target_address = self.storage_registry.get_node_address(target_node_id)
        
        if not primary_address or not target_address:
            logger.error(f"[HealthMonitor] Could not find addresses - primary: {primary_address}, target: {target_address}")
            return False
        
        try:
            logger.info(f"[HealthMonitor][REPLICATION] Starting data replication from {primary_id} ({primary_address}) to {target_node_id} ({target_address})")
            
            # Connect to primary to get all items
            with grpc.insecure_channel(primary_address) as channel:
                stub = mktplace_pb2_grpc.MarketplaceServiceStub(channel)
                
                # Get all items from primary using SearchItems (returns all items when no filters)
                logger.info(f"[HealthMonitor][REPLICATION] Fetching all items from primary node {primary_id}")
                search_response = stub.SearchItems(
                    mktplace_pb2.SearchItemsRequest(category='', keyword=''),
                    timeout=30
                )
                
                items = search_response.items
                logger.info(f"[HealthMonitor][REPLICATION] Retrieved {len(items)} items from primary")
                
                if not items:
                    logger.info(f"[HealthMonitor][REPLICATION] No items to replicate, primary is empty")
                    return True
                
                # Now replicate each item to the target node
                logger.info(f"[HealthMonitor][REPLICATION] Replicating {len(items)} items to target node {target_node_id}")
                with grpc.insecure_channel(target_address) as target_channel:
                    target_stub = mktplace_pb2_grpc.MarketplaceServiceStub(target_channel)
                    
                    success_count = 0
                    for i, item in enumerate(items, 1):
                        try:
                            create_request = mktplace_pb2.CreateItemRequest(
                                item_id=item.item_id,
                                seller_id=item.seller_id,
                                title=item.title,
                                category=item.category,
                                description=item.description,
                                starting_price=item.starting_price,
                                quantity=item.quantity
                            )
                            
                            response = target_stub.CreateItem(create_request, timeout=5)
                            if response.success:
                                success_count += 1
                            else:
                                logger.warning(f"[HealthMonitor][REPLICATION] Failed to create item {item.item_id} on target")
                            
                            if i % 10 == 0:
                                logger.info(f"[HealthMonitor][REPLICATION] Progress: {i}/{len(items)} items replicated")
                        
                        except Exception as e:
                            logger.error(f"[HealthMonitor][REPLICATION] Error replicating item {item.item_id}: {e}")
                    
                    logger.info(f"[HealthMonitor][REPLICATION] Replication complete: {success_count}/{len(items)} items successfully replicated")
                    return success_count == len(items)
        
        except Exception as e:
            logger.error(f"[HealthMonitor][REPLICATION] Error during replication: {e}", exc_info=True)
            return False
    
    def stop(self):
        """Stop the health monitor."""
        self.running = False
        logger.info("Health monitor stopped")
