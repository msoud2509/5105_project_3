import grpc
import time
import os
import logging
from concurrent import futures

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import generated protobuf classes
try:
    from gRPC import mktplace_pb2
    from gRPC import mktplace_pb2_grpc
    from gRPC import service_node_pb2
    from gRPC import service_node_pb2_grpc
    import threading
except ImportError:
    logger.warning("Proto files not found. See README.md for instructions on generating gRPC code.")

from .registries import StorageNodeRegistry, ServiceNodeRegistry
from .heartbeat import HealthMonitor
from .scaling_manager import ServiceScalingManager


class MarketplaceController(mktplace_pb2_grpc.MarketplaceServiceServicer):
    """Controller that routes requests to service nodes."""
    
    def __init__(self, service_registry):
        self.service_registry = service_registry

        #have a count_lock to protect request count and window start time
        self._count_lock = threading.Lock()
        self._request_count = 0
        self._window_start = time.time()

    def get_request_rate(self) -> float:
        """Returns requests/sec since last call, then resets the window."""
        with self._count_lock:
            now = time.time()
            elapsed = now - self._window_start
            rate = self._request_count / elapsed if elapsed > 0 else 0
            # Reset for next window
            self._request_count = 0
            self._window_start = now
            return rate
        

    def _forward_to_node(self, method_name, request):
        """Forward a request to a healthy service node."""
        node_id, node_info = self.service_registry.get_healthy_node()
        if node_id == None:
            raise grpc.RpcError(grpc.StatusCode.UNAVAILABLE, "No healthy service nodes available")
        
        address = node_info['address']
        
        try:
            with grpc.insecure_channel(address) as channel:
                stub = mktplace_pb2_grpc.MarketplaceServiceStub(channel)
                
                # call the appropriate method on the service node
                method = getattr(stub, method_name)
                response = method(request, timeout=10)
                # Increment request count for scaling decisions
                with self._count_lock:
                    self._request_count += 1
                    
                return response
        except grpc.RpcError as e:
            # mark the node as unhealthy if it fails
            self.service_registry.mark_healthy(node_id, False)
            raise grpc.RpcError(grpc.StatusCode.UNAVAILABLE, f"Service node {node_id} failed")
    
    ################################## Below are gRPC forwards ##################################
    def CreateItem(self, request, context):
        """Forward CreateItem request to a service node."""
        try:
            logger.info(f"Received CreateItem request for item_id={request.item_id}")
            return self._forward_to_node('CreateItem', request)
        except Exception as e:
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return mktplace_pb2.CreateItemResponse()
    
    def GetItem(self, request, context):
        """Forward GetItem request to a service node."""
        try:
            logger.info(f"Received GetItem request for item_id={request.item_id}")
            return self._forward_to_node('GetItem', request)
        except Exception as e:
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return mktplace_pb2.GetItemResponse()
    
    def SearchItems(self, request, context):
        """Forward SearchItems request to a service node."""
        try:
            logger.info(f"Received SearchItems request")
            return self._forward_to_node('SearchItems', request)
        except Exception as e:
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return mktplace_pb2.SearchItemsResponse()
    
    def UpdateItem(self, request, context):
        """Forward UpdateItem request to a service node."""
        try:
            logger.info(f"Received UpdateItem request for item_id={request.item_id}")
            return self._forward_to_node('UpdateItem', request)
        except Exception as e:
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return mktplace_pb2.UpdateItemResponse()
    
    def PlaceBid(self, request, context):
        """Forward PlaceBid request to a service node."""
        try:
            logger.info(f"Received PlaceBid request for item_id={request.item_id}")
            return self._forward_to_node('PlaceBid', request)
        except Exception as e:
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return mktplace_pb2.PlaceBidResponse()
        
    def JoinAuction(self, request_iterator, context):
        """Forward a bidirectional stream to a service node."""
        node_id, node_info = self.service_registry.get_healthy_node()
        if node_id is None:
            context.set_details("No healthy service nodes available")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return

        address = node_info['address']
        try:
            with grpc.insecure_channel(address) as channel:
                stub = mktplace_pb2_grpc.MarketplaceServiceStub(channel)
                # Forward the input stream and yield from the output stream
                for response in stub.JoinAuction(request_iterator):
                    yield response
        except Exception as e:
            logger.error(f"Auction stream failed: {e}")
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
        
    
        
    ################################## Above are gRPC forwards ##################################


        

def start_controller(host='localhost', port=50051, 
                    service_nodes=None, storage_nodes=None):
    """
    Start the marketplace controller server.
    
    Args:
        host: Controller host address
        port: Controller port
        service_nodes: List of service node addresses
        storage_nodes: Dict of {node_id: address} for storage nodes
    """
    
    storage_registry = StorageNodeRegistry()
    service_registry = ServiceNodeRegistry(storage_registry)
    
    # Register storage nodes
    if storage_nodes:
        for node_id, address in storage_nodes.items():
            storage_registry.add_node(node_id, address)
        
        # Elect initial primary (highest ID among initial nodes)
        primary = max(storage_nodes.keys())
        storage_registry.set_primary(primary)
        logger.info(f"Initial primary: storage node {primary}")
    
    # Register service nodes and send them their storage node assignments
    service_node_addresses = {}
    if service_nodes:
        for i, node_address in enumerate(service_nodes):
            service_id, assigned_storage_id = service_registry.add_service_node(node_address)
            service_node_addresses[service_id] = node_address
            
            # send storage node assignments to service node
            try:
                channel = grpc.insecure_channel(node_address)
                stub = service_node_pb2_grpc.ServiceNodeControlStub(channel)
                
                assigned_address = storage_registry.get_node_address(assigned_storage_id)
                request = service_node_pb2.AssignStorageNodeRequest(
                    service_node_id=service_id,
                    storage_node_address=assigned_address,
                    storage_node_id=assigned_storage_id
                )
                response = stub.AssignStorageNode(request, timeout=10)
                
                if response.success:
                    logger.info(f"Service node {service_id} assigned to storage {assigned_storage_id}")
                else:
                    logger.error(f"Failed to assign service node {service_id}")
                
                channel.close()
            except Exception as e:
                logger.error(f"Error assigning service node {service_id}: {e}")
    
    # Start health monitor for storage nodes
    # Pass service_node_addresses for failover updates
    monitor = HealthMonitor(storage_registry, service_registry, service_node_addresses)
    monitor.start()
    
    # Start service node scaling manager
    controller = MarketplaceController(service_registry)

    scaling_manager = ServiceScalingManager(
        service_registry=service_registry,
        storage_registry=storage_registry,
        service_node_addresses=service_node_addresses,
        controller=controller,  # now this works
        check_interval=int(os.getenv('SCALE_CHECK_INTERVAL', '10')),
        min_nodes=int(os.getenv('MIN_SERVICE_NODES', '3')),
        max_nodes=int(os.getenv('MAX_SERVICE_NODES', '10'))
    )
    scaling_manager.start()
    
    # Create server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mktplace_pb2_grpc.add_MarketplaceServiceServicer_to_server(
        controller, server
    )
    
    server.add_insecure_port(f'{host}:{port}')
    
    logger.info(f"Starting controller on {host}:{port}")
    server.start()
    
    try:
        while True:
            time.sleep(86400)  # Keep running
    except KeyboardInterrupt:
        monitor.stop()
        scaling_manager.stop()
        server.stop(0)
        logger.info("Controller stopped")


if __name__ == '__main__':
    # system will only run in docker, no need to use localhost
    service_nodes = [
        'service-node-0:50051',
        'service-node-1:50051',
        'service-node-2:50051',
    ]
    
    storage_nodes = {
        0: 'storage-node-0:50051',
        1: 'storage-node-1:50051',
        2: 'storage-node-2:50051',
    }
    
    host = '0.0.0.0'
    port = 50051
    
    start_controller(host=host, port=port, service_nodes=service_nodes, storage_nodes=storage_nodes)
