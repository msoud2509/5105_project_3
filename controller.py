import grpc
import threading
import time
from concurrent import futures
from threading import Lock

# Import generated protobuf classes (see README.md to create mktplace_pb2 files)
try:
    from gRPC import mktplace_pb2
    from gRPC import mktplace_pb2_grpc
except ImportError:
    print("Warning: mktplace_pb2 not found. See README.md for instructions on generating gRPC code.")


class StorageNodeRegistry:
    """Registry to manage storage nodes and their health statuses."""

    def __init__(self):
        self.lock = Lock()
        self.nodes = {}  # {node_id: {'address': str, 'healthy': bool}}
        self.index = 0  # For round-robin load balancing
        
    def add_node(self, node_id: int, address: str):
        """Register a storage node."""
        with self.lock:
            self.nodes[node_id] = {
                'address': address,
                'healthy': True
            }
    
    def mark_healthy(self, node_id: int, healthy: bool):
        """Mark a node as healthy or unhealthy."""
        with self.lock:
            if node_id in self.nodes:
                self.nodes[node_id]['healthy'] = healthy
    
    def get_healthy_node(self):
        """Get the next healthy node using round-robin, or None if all are down."""
        with self.lock:
            if not self.nodes:
                return None, None
            
            node_ids = list(self.nodes.keys())
            num_nodes = len(node_ids)
            
            # Try to find a healthy node starting from the current index
            for _ in range(num_nodes):
                current_node_id = node_ids[self.index % num_nodes]
                self.index = (self.index + 1) % num_nodes
                
                if self.nodes[current_node_id]['healthy']:
                    return current_node_id, self.nodes[current_node_id]
            
            # No healthy nodes found
            return None, None
    
    def get_node_address(self, node_id: int):
        """Get the address of a node."""
        with self.lock:
            if node_id in self.nodes:
                return self.nodes[node_id]['address']
        return None


class MarketplaceController(mktplace_pb2_grpc.MarketplaceServiceServicer):
    """Controller that routes requests to service nodes."""
    
    def __init__(self, registry):
        self.registry = registry
    
    def _forward_to_node(self, method_name, request):
        """Forward a request to a healthy service node."""
        node_id, node_info = self.registry.get_healthy_node()
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
            self.registry.mark_healthy(node_id, False)
            raise grpc.RpcError(grpc.StatusCode.UNAVAILABLE, f"Service node {node_id} failed")
    
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


class HealthMonitor(threading.Thread):
    """Background thread that monitors storage node health via heartbeats."""
    
    def __init__(self, registry, heartbeat_interval=5):
        super().__init__(daemon=True)
        self.registry = registry
        self.heartbeat_interval = heartbeat_interval
        self.running = True
    
    def run(self):
        """Periodically check health of all nodes."""
        while self.running:
            with self.registry.lock:
                nodes_to_check = list(self.registry.nodes.items())
            
            for node_id, node_info in nodes_to_check:
                try:
                    # Simple health check: try to connect
                    with grpc.insecure_channel(node_info['address']) as channel:
                        stub = mktplace_pb2_grpc.MarketplaceServiceStub(channel)
                    self.registry.mark_healthy(node_id, True)
                except Exception:
                    # Mark node as unhealthy if we can't reach it
                    self.registry.mark_healthy(node_id, False)
            
            time.sleep(self.heartbeat_interval)
    
    def stop(self):
        """Stop the health monitor."""
        self.running = False


def start_controller(host='localhost', port=50051, service_nodes=None):
    """Start the marketplace controller server."""
    
    registry = StorageNodeRegistry()
    
    if service_nodes:
        for i, node_address in enumerate(service_nodes):
            registry.add_node(i, node_address)
    
    # Start health monitor
    monitor = HealthMonitor(registry)
    monitor.start()
    
    # create server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mktplace_pb2_grpc.add_MarketplaceServiceServicer_to_server(
        MarketplaceController(registry), server
    )
    
    server.add_insecure_port(f'{host}:{port}')
    
    print(f"Controller starting on {host}:{port}")
    server.start()
    
    try:
        while True:
            time.sleep(86400)  # Keep running
    except KeyboardInterrupt:
        monitor.stop()
        server.stop(0)
        print("Controller stopped")


if __name__ == '__main__':
    service_nodes = [
        'localhost:50052',  # service node 1
        'localhost:50053',  # service node 2
    ]
    start_controller(service_nodes=service_nodes)
