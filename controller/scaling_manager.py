"""Service node scaling manager that monitors metrics and scales based on load."""

import time
import logging
import docker
import grpc
import threading
from docker.errors import DockerException
from typing import Dict, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from gRPC import service_node_pb2
    from gRPC import service_node_pb2_grpc
except ImportError:
    logger.warning("Proto files not found")


class ServiceScalingManager(threading.Thread):
    """Manages dynamic scaling of service nodes based on traffic metrics."""
    
    def __init__(self, 
                 service_registry,
                 storage_registry,
                 service_node_addresses: Dict[int, str],
                 controller,
                 check_interval: int = 10,
                 min_nodes: int = 3,
                 max_nodes: int = 10):
        """
        Initialize the scaling manager.
        
        Args:
            service_registry: ServiceNodeRegistry for managing service nodes
            storage_registry: StorageNodeRegistry for managing storage nodes
            service_node_addresses: Dict mapping service_id to address
            check_interval: How often to check metrics (seconds)
            min_nodes: Minimum service nodes to maintain
            max_nodes: Maximum service nodes allowed
        """
        super().__init__(daemon=True)
        self.controller = controller
        self.scale_up_threshold = 10.0  # req/sec
        self.scale_down_threshold = 2.0  # req/sec
        self._cooldown = 30  # seconds between scaling actions
        self._last_scale_time = 0
        self.service_registry = service_registry
        self.storage_registry = storage_registry
        self.service_node_addresses = service_node_addresses
        self.check_interval = check_interval
        self.min_nodes = min_nodes
        self.max_nodes = max_nodes
        self.running = True
        self.next_service_node_id = 3  # Start after initial 3 nodes (0, 1, 2)
        
        # Connect to Docker daemon
        try:
            self.docker_client = docker.from_env()
            logger.info("[ScalingManager] Connected to Docker daemon")
        except DockerException as e:
            logger.error(f"[ScalingManager] Failed to connect to Docker: {e}")
            self.docker_client = None

    
    def run(self):
        if not self.docker_client:
            logger.error("[ScalingManager] Docker client not available, scaling disabled")
            return
        
        logger.info("[ScalingManager] Starting scaling manager")
        
        while self.running:
            try:
                self._check_and_scale()
            except Exception as e:
                logger.error(f"[ScalingManager] Error in scaling loop: {e}")
            
            time.sleep(self.check_interval)
    
    def _check_and_scale(self):
        """Check metrics and scale up/down as needed."""
        current_count = self._get_service_node_count()
        logger.info(f"[ScalingManager] Current service nodes: {current_count}")
        
        # TODO: Implement actual metrics collection
        # For now, use a placeholder metric (always maintain min_nodes)
        metrics = self._collect_metrics()
        
        scale_action = self._determine_scale_action(current_count, metrics)
        
        if scale_action == 'scale_up':
            self._scale_up(current_count)
        elif scale_action == 'scale_down':
            self._scale_down(current_count)
    
    def _get_service_node_count(self) -> int:
        """Get current number of running service node containers."""
        try:
            containers = self.docker_client.containers.list(
                filters={'label': 'service_node=true'},
                all=False
            )
            return len(containers)
        except Exception as e:
            logger.error(f"[ScalingManager] Error counting service nodes: {e}")
            return self.min_nodes
    
    # def _collect_metrics(self) -> Dict:
    #     """
    #     Collect metrics from service nodes.
        
    #     TODO: Implement actual metrics collection:
    #     - CPU usage
    #     - Memory usage
    #     - Request count
    #     - Response times
    #     """
    #     # Placeholder: return empty metrics dict
    #     return {
    #         'cpu_avg': 0,
    #         'memory_avg': 0,
    #         'request_rate': 0,
    #         'p95_latency': 0
    #     }
    def _collect_metrics(self) -> Dict:
        rate = 0.0
        if self.controller:
            rate = self.controller.get_request_rate()
            logger.info(f"[ScalingManager] Request rate: {rate:.2f} req/sec")
        return {'request_rate': rate}
    
    # def _determine_scale_action(self, current_count: int, metrics: Dict) -> str:
    #     """
    #     Determine if we should scale up, down, or maintain.
        
    #     TODO: Implement metrics-based decision logic:
    #     - If CPU > 80% or requests > threshold: scale_up
    #     - If CPU < 20% and requests < threshold: scale_down
    #     - Otherwise: maintain
    #     """
    #     # For now, just maintain minimum
    #     if current_count < self.min_nodes:
    #         return 'scale_up'
    #     elif current_count > self.min_nodes:
    #         return 'scale_down'
    #     return 'maintain'
    def _determine_scale_action(self, current_count: int, metrics: Dict) -> str:
        rate = metrics.get('request_rate', 0)
        now = time.time()

        # Cooldown: don't scale too frequently
        if now - self._last_scale_time < self._cooldown:
            return 'maintain'

        if rate > self.scale_up_threshold and current_count < self.max_nodes:
            self._last_scale_time = now
            logger.info(f"[ScalingManager] High load ({rate:.2f} req/s) → scale up")
            return 'scale_up'
        
        elif rate < self.scale_down_threshold and current_count > self.min_nodes:
            self._last_scale_time = now
            logger.info(f"[ScalingManager] Low load ({rate:.2f} req/s) → scale down")
            return 'scale_down'
        
        return 'maintain'
    
    def _scale_up(self, current_count: int):
        """Scale up by creating a new service node container and adding it to the registry."""
        if current_count >= self.max_nodes:
            logger.warning(f"[ScalingManager] Already at max nodes ({self.max_nodes})")
            return
        
        new_node_id = self.next_service_node_id
        self.next_service_node_id += 1
        new_node_address = f'service-node-{new_node_id}:50051'
        
        try:
            logger.info(f"[ScalingManager] Scaling up: creating service-node-{new_node_id}")
            
            # Get docker network
            networks = self.docker_client.networks.list(
                filters={'name': 'marketplace-network'}
            )
            network = networks[0] if networks else None
            
            if not network:
                logger.error("[ScalingManager] Could not find marketplace-network")
                return
            
            # Create container
            container = self.docker_client.containers.run(
                'marketplace-project_3-service-node-0:latest',  # Use latest image
                detach=True,
                name=f'service-node-{new_node_id}',
                environment=[
                    f'NODE_ID={new_node_id}',
                    'GRPC_HOST=0.0.0.0',
                    'GRPC_PORT=50051',
                    'CONTROLLER_HOST=controller',
                    'CONTROLLER_PORT=50051',
                ],
                healthcheck={
                    'test': ['CMD', 'python', '-c', "import grpc; grpc.insecure_channel('localhost:50051')"],
                    'interval': 5000000000,  # 5 seconds in nanoseconds
                    'timeout': 2000000000,   # 2 seconds
                    'retries': 2,
                    'start_period': 10000000000,  # 10 seconds
                },
                labels={'service_node': 'true'},
                restart_policy={'Name': 'unless-stopped', 'MaximumRetryCount': 0},
            )
            
            # Connect to network
            network.connect(container)
            
            logger.info(f"[ScalingManager] Created service-node-{new_node_id}")
            
            # Give container time to start
            time.sleep(2)
            
            # Register in service registry and assign a storage node
            try:
                service_id, assigned_storage_id = self.service_registry.add_service_node(new_node_address)
                assigned_address = self.storage_registry.get_node_address(assigned_storage_id)
                
                logger.info(f"[ScalingManager] Registered service-node-{new_node_id} (id={service_id}) to storage {assigned_storage_id}")
                
                # Send assignment to the new service node
                channel = grpc.insecure_channel(new_node_address)
                stub = service_node_pb2_grpc.ServiceNodeControlStub(channel)
                
                request = service_node_pb2.AssignStorageNodeRequest(
                    service_node_id=service_id,
                    storage_node_address=assigned_address,
                    storage_node_id=assigned_storage_id
                )
                response = stub.AssignStorageNode(request, timeout=10)
                channel.close()
                
                if response.success:
                    logger.info(f"[ScalingManager] Successfully assigned service-node-{new_node_id} to storage {assigned_storage_id}")
                else:
                    logger.error(f"[ScalingManager] Failed to assign storage for service-node-{new_node_id}")
            
            except Exception as e:
                logger.error(f"[ScalingManager] Error registering or assigning service-node-{new_node_id}: {e}")
        
        except Exception as e:
            logger.error(f"[ScalingManager] Error scaling up: {e}")
    
    def _scale_down(self, current_count: int):
        """Scale down by removing a service node container (not one of the initial 3)."""
        if current_count <= self.min_nodes:
            logger.warning(f"[ScalingManager] Already at min nodes ({self.min_nodes})")
            return
        
        try:
            # Get the highest numbered (newest) service node to remove
            containers = self.docker_client.containers.list(
                filters={'label': 'service_node=true'},
                all=False
            )
            
            # Sort by name and pick the last one
            containers.sort(key=lambda c: int(c.name.split('-')[-1]))
            
            if containers:
                container_to_remove = containers[-1]
                node_id = int(container_to_remove.name.split('-')[-1])
                container_address = f'service-node-{node_id}:50051'
                
                # Only remove if not one of initial 3
                if node_id >= 3:
                    logger.info(f"[ScalingManager] Scaling down: removing {container_to_remove.name}")
                    
                    # deregister from registry by finding service_id via address
                    service_id = self.service_registry.get_service_node_by_address(container_address)
                    
                    if service_id is not None:
                        removed = self.service_registry.remove_service_node(service_id)
                        if removed:
                            logger.info(f"[ScalingManager] Deregistered service node {service_id} from registry")
                            
                            # Remove from service_node_addresses
                            if service_id in self.service_node_addresses:
                                del self.service_node_addresses[service_id]
                        else:
                            logger.warning(f"[ScalingManager] Failed to remove service node {service_id} from registry")
                    else:
                        logger.warning(f"[ScalingManager] Could not find service_id for address {container_address}")
                    
                    # Remove the container
                    container_to_remove.stop()
                    container_to_remove.remove()
                    logger.info(f"[ScalingManager] Removed {container_to_remove.name}")
        
        except Exception as e:
            logger.error(f"[ScalingManager] Error scaling down: {e}")
    
    def stop(self):
        """Stop the scaling manager."""
        self.running = False
        logger.info("[ScalingManager] Stopped")
