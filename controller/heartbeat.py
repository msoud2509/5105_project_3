"""Health monitoring and primary election for storage nodes."""

import grpc
import threading
import time
import logging
import sys
from typing import Optional, Dict

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
                    # Use GetItem as a simple heartbeat check
                    response = stub.GetItem(
                        mktplace_pb2.GetItemRequest(item_id="__heartbeat__"),
                        timeout=2
                    )
                    self.storage_registry.mark_healthy(node_id, True)
                    logger.info(f"Heartbeat to storage node {node_id} successful")
            except Exception as e:
                logger.error(f"Heartbeat to storage node {node_id} failed: {e}")
                self.storage_registry.mark_healthy(node_id, False)
                
                # If primary failed, trigger failover
                if node_id == self.storage_registry.get_primary():
                    logger.warning(f"Primary node {node_id} is down! Initiating failover...")
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
        Handle storage node failure:
        1. Elect a new primary
        2. Redistribute service nodes
        3. Notify affected service nodes of reassignment
        """
        # Elect new primary
        self._elect_primary()
        
        # Get affected service nodes
        affected_services = self.service_registry.get_services_on_storage(failed_storage_id)
        
        if not affected_services:
            logger.info(f"No service nodes affected by storage node {failed_storage_id} failure")
            return
        
        # Redistribute service nodes that were attached to failed storage node
        self.service_registry.redistribute_services(failed_storage_id)
        
        # Notify each affected service node of their new storage assignment
        for service_id in affected_services:
            new_storage_id = self.service_registry.get_service_node_storage(service_id)
            if new_storage_id is None:
                logger.error(f"Could not determine new storage for service {service_id}")
                continue
            
            new_storage_address = self.storage_registry.get_node_address(new_storage_id)
            if new_storage_address is None:
                logger.error(f"Could not find address for storage node {new_storage_id}")
                continue
            
            # Send update to service node
            service_address = self.service_node_addresses.get(service_id)
            if not service_address:
                logger.error(f"Could not find address for service node {service_id}")
                continue
            
            try:
                channel = grpc.insecure_channel(service_address)
                stub = service_node_pb2_grpc.ServiceNodeControlStub(channel)
                
                request = service_node_pb2.UpdateStorageNodeRequest(
                    service_node_id=service_id,
                    storage_node_address=new_storage_address,
                    storage_node_id=new_storage_id
                )
                response = stub.UpdateStorageNode(request, timeout=10)
                
                if response.success:
                    logger.info(f"Updated service node {service_id} to storage {new_storage_id}")
                else:
                    logger.error(f"Failed to update service node {service_id}")
                
                channel.close()
            except Exception as e:
                logger.error(f"Error updating service node {service_id}: {e}")
    
    def stop(self):
        """Stop the health monitor."""
        self.running = False
        logger.info("Health monitor stopped")
