import grpc
import time
from concurrent import futures

# Import generated protobuf classes
try:
    from gRPC import mktplace_pb2
    from gRPC import mktplace_pb2_grpc
except ImportError:
    print("Warning: mktplace_pb2 not found. See README.md for instructions on generating gRPC code.")

from .registries import StorageNodeRegistry, ServiceNodeRegistry
from .heartbeat import HealthMonitor

class MarketplaceController(mktplace_pb2_grpc.MarketplaceServiceServicer):
    """Controller that routes requests to service nodes."""
    
    def __init__(self, service_registry):
        self.service_registry = service_registry
    
    def _forward_to_node(self, method_name, request):
        """Forward a request to a healthy service node."""
        node_id, node_info = self.service_registry.get_healthy_node()
        if not node_id:
            raise grpc.RpcError(grpc.StatusCode.UNAVAILABLE, "No healthy service nodes available")
        
        address = node_info['address']
        
        try:
            with grpc.insecure_channel(address) as channel:
                stub = mktplace_pb2_grpc.MarketplaceServiceStub(channel)
                
                # call the appropriate method on the service node
                method = getattr(stub, method_name)
                response = method(request, timeout=10)
                return response
        except grpc.RpcError as e:
            # mark the node as unhealthy if it fails
            self.service_registry.mark_healthy(node_id, False)
            raise grpc.RpcError(grpc.StatusCode.UNAVAILABLE, f"Service node {node_id} failed")
    
    ################################## Below are gRPC forwards ##################################
    def CreateItem(self, request, context):
        """Forward CreateItem request to a service node."""
        try:
            return self._forward_to_node('CreateItem', request)
        except Exception as e:
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return mktplace_pb2.CreateItemResponse()
    
    def GetItem(self, request, context):
        """Forward GetItem request to a service node."""
        try:
            return self._forward_to_node('GetItem', request)
        except Exception as e:
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return mktplace_pb2.GetItemResponse()
    
    def SearchItems(self, request, context):
        """Forward SearchItems request to a service node."""
        try:
            return self._forward_to_node('SearchItems', request)
        except Exception as e:
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return mktplace_pb2.SearchItemsResponse()
    
    def UpdateItem(self, request, context):
        """Forward UpdateItem request to a service node."""
        try:
            return self._forward_to_node('UpdateItem', request)
        except Exception as e:
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return mktplace_pb2.UpdateItemResponse()
    
    def PlaceBid(self, request, context):
        """Forward PlaceBid request to a service node."""
        try:
            return self._forward_to_node('PlaceBid', request)
        except Exception as e:
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return mktplace_pb2.PlaceBidResponse()


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
    
    # Initialize registries
    storage_registry = StorageNodeRegistry()
    service_registry_for_routing = ServiceNodeRegistry_Old()
    service_registry = ServiceNodeRegistry(storage_registry)
    
    # Register storage nodes
    if storage_nodes:
        for node_id, address in storage_nodes.items():
            storage_registry.add_node(node_id, address)
        
        # Elect initial primary (highest ID among initial nodes)
        primary = max(storage_nodes.keys())
        storage_registry.set_primary(primary)
        print(f"[Controller] Initial primary: storage node {primary}")
    
    # Register service nodes
    if service_nodes:
        for i, node_address in enumerate(service_nodes):
            service_registry_for_routing.add_node(i, node_address)
            # Also add to service-storage registry for assignment tracking
            service_registry.add_service_node(node_address)
    
    # Start health monitor for storage nodes
    monitor = HealthMonitor(storage_registry, service_registry)
    monitor.start()
    
    # Create server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mktplace_pb2_grpc.add_MarketplaceServiceServicer_to_server(
        MarketplaceController(service_registry_for_routing), server
    )
    
    server.add_insecure_port(f'{host}:{port}')
    
    print(f"[Controller] Starting on {host}:{port}")
    server.start()
    
    try:
        while True:
            time.sleep(86400)  # Keep running
    except KeyboardInterrupt:
        monitor.stop()
        server.stop(0)
        print("[Controller] Stopped")


if __name__ == '__main__':
    service_nodes = [
        'localhost:50052',  # service node 1
        'localhost:50053',  # service node 2
    ]
    
    storage_nodes = {
        0: 'localhost:50060',
        1: 'localhost:50061',
        2: 'localhost:50062',
    }
    
    start_controller(service_nodes=service_nodes, storage_nodes=storage_nodes)
