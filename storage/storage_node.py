import grpc
import threading
import time
import json
import os
from concurrent import futures
from threading import Lock
from typing import Dict
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from gRPC import mktplace_pb2
    from gRPC import mktplace_pb2_grpc
except ImportError:
    print("Warning: gRPC modules not found. See README.md for instructions on generating gRPC code.")


class StorageNode(mktplace_pb2_grpc.MarketplaceServiceServicer):
    """Storage node with primary-backup replication. Controller manages fault tolerance."""
    
    def __init__(self, node_id: int, replica_addresses: Dict[int, str], is_primary: bool = False):
        self.node_id = node_id
        self.is_primary = is_primary
        self.primary_node_id = 2  # default primary is node 2
        self.replica_addresses = replica_addresses
        
        # Local data store
        self.data = {}  # {item_id: item_dict}
        self.data_lock = Lock()
        
        self.running = True
    
    def startup(self):
        """If primary, load data from file and replicate to backups. Otherwise, just start."""
        if self.is_primary:
            self._load_data_from_file()
            self._replicate_data_to_backups()
        
        print(f'[Node {self.node_id}] Storage node started (Primary: {self.is_primary})')
    
    def shutdown(self):
        self.running = False
    
    def _load_data_from_file(self):
        """ON STARTUP: Load marketplace data from JSONL file."""
        data_file = '/app/data/marketplace_items.jsonl'
        if os.path.exists(data_file):
            with open(data_file) as f:
                for line in f:
                    if line.strip():
                        item = json.loads(line)
                        with self.data_lock:
                            self.data[item['item_id']] = item
            print(f"[Node {self.node_id}] Loaded {len(self.data)} items from file")
    
    def _replicate_data_to_backups(self):
        """ON STARTUP: Replicate all data to backup nodes (if primary)."""
        print(f"[Node {self.node_id}] Replicating {len(self.data)} items to backups")
        
        for node_id, address in self.replica_addresses.items():
            if node_id != self.node_id:
                try:
                    with grpc.insecure_channel(address) as channel:
                        stub = mktplace_pb2_grpc.MarketplaceServiceStub(channel)
                        
                        # Send each item as a CreateItem call
                        for item_id, item_data in self.data.items():
                            request = self._dict_to_create_item_request(item_data)
                            try:
                                stub.CreateItem(request, timeout=5)
                            except Exception as e:
                                print(f"[Node {self.node_id}] Failed to replicate item {item_id} to node {node_id}: {e}")
                except Exception as e:
                    print(f"[Node {self.node_id}] Failed to connect to node {node_id} at {address}: {e}")
    
    def _dict_to_create_item_request(self, item_dict):
        """Convert item dict to CreateItemRequest."""
        return mktplace_pb2.CreateItemRequest(
            item_id=item_dict.get('item_id', ''),
            seller_id=item_dict.get('seller_id', ''),
            title=item_dict.get('title', ''),
            category=item_dict.get('category', ''),
            description=item_dict.get('description', ''),
            starting_price=item_dict.get('starting_price', 0.0),
            quantity=item_dict.get('quantity', 0)
        )
    

    ################################### All below are gRPC method implementations ###################################
    def CreateItem(self, request, context):
        try:
            # If not primary, forward to primary
            if not self.is_primary:
                primary_address = self.replica_addresses.get(self.primary_node_id)
                if primary_address:
                    try:
                        with grpc.insecure_channel(primary_address) as channel:
                            stub = mktplace_pb2_grpc.MarketplaceServiceStub(channel)
                            return stub.CreateItem(request, timeout=5)
                    except Exception as e:
                        print(f"[Node {self.node_id}] Failed to forward CreateItem to primary: {e}")
                        context.set_details(f"Primary unavailable")
                        context.set_code(grpc.StatusCode.UNAVAILABLE)
                        return mktplace_pb2.CreateItemResponse(success=False)
            
            # I'm primary, replicate to all backups
            self._replicate_write_to_backups('CreateItem', request)
            
            # Store locally
            item_dict = {
                'item_id': request.item_id,
                'seller_id': request.seller_id,
                'title': request.title,
                'category': request.category,
                'description': request.description,
                'starting_price': request.starting_price,
                'current_price': request.starting_price,
                'quantity': request.quantity,
                'status': 'ACTIVE',
                'version': 1
            }
            
            with self.data_lock:
                self.data[request.item_id] = item_dict
            
            return mktplace_pb2.CreateItemResponse(success=True, item_id=request.item_id)
        
        except Exception as e:
            print(f"[Node {self.node_id}] CreateItem error: {e}")
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return mktplace_pb2.CreateItemResponse(success=False)
    
    def GetItem(self, request, context):
        """Handle GetItem request."""
        try:
            with self.data_lock:
                if request.item_id in self.data:
                    item = self.data[request.item_id]
                    return mktplace_pb2.GetItemResponse(
                        found=True,
                        item_id=item.get('item_id', ''),
                        seller_id=item.get('seller_id', ''),
                        title=item.get('title', ''),
                        category=item.get('category', ''),
                        current_price=item.get('current_price', 0.0),
                        quantity=item.get('quantity', 0),
                        status=item.get('status', '')
                    )
                else:
                    return mktplace_pb2.GetItemResponse(found=False)
        
        except Exception as e:
            print(f"[Node {self.node_id}] GetItem error: {e}")
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return mktplace_pb2.GetItemResponse(found=False)
    
    def SearchItems(self, request, context):
        try:
            results = []
            with self.data_lock:
                for item_id, item in self.data.items():
                    if request.category and item.get('category') != request.category:
                        continue
                    if request.search_term and request.search_term.lower() not in item.get('title', '').lower():
                        continue
                    results.append(item)
            
            return mktplace_pb2.SearchItemsResponse(
                count=len(results),
                items=[mktplace_pb2.ItemInfo(
                    item_id=item.get('item_id', ''),
                    title=item.get('title', ''),
                    current_price=item.get('current_price', 0.0)
                ) for item in results[:100]]  # Limit to 100 results
            )
        
        except Exception as e:
            print(f"[Node {self.node_id}] SearchItems error: {e}")
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return mktplace_pb2.SearchItemsResponse(count=0)
    
    def UpdateItem(self, request, context):
        """Handle UpdateItem request."""
        try:
            # If not primary, forward to primary
            if not self.is_primary:
                primary_address = self.replica_addresses.get(self.primary_node_id)
                if primary_address:
                    try:
                        with grpc.insecure_channel(primary_address) as channel:
                            stub = mktplace_pb2_grpc.MarketplaceServiceStub(channel)
                            return stub.UpdateItem(request, timeout=5)
                    except Exception as e:
                        print(f"[Node {self.node_id}] Failed to forward UpdateItem to primary: {e}")
                        context.set_details(f"Primary unavailable")
                        context.set_code(grpc.StatusCode.UNAVAILABLE)
                        return mktplace_pb2.UpdateItemResponse(success=False)
            
            # I'm primary, replicate to all backups
            self._replicate_write_to_backups('UpdateItem', request)
            
            with self.data_lock:
                if request.item_id in self.data:
                    item = self.data[request.item_id]
                    item['current_price'] = request.current_price
                    item['quantity'] = request.quantity
                    item['status'] = request.status
                    item['version'] = item.get('version', 0) + 1
                    return mktplace_pb2.UpdateItemResponse(success=True, version=item['version'])
                else:
                    return mktplace_pb2.UpdateItemResponse(success=False)
        
        except Exception as e:
            print(f"[Node {self.node_id}] UpdateItem error: {e}")
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return mktplace_pb2.UpdateItemResponse(success=False)
    
    def PlaceBid(self, request, context):
        """Handle PlaceBid request."""
        try:
            # If not primary, forward to primary
            if not self.is_primary:
                primary_address = self.replica_addresses.get(self.primary_node_id)
                if primary_address:
                    try:
                        with grpc.insecure_channel(primary_address) as channel:
                            stub = mktplace_pb2_grpc.MarketplaceServiceStub(channel)
                            return stub.PlaceBid(request, timeout=5)
                    except Exception as e:
                        print(f"[Node {self.node_id}] Failed to forward PlaceBid to primary: {e}")
                        context.set_details(f"Primary unavailable")
                        context.set_code(grpc.StatusCode.UNAVAILABLE)
                        return mktplace_pb2.PlaceBidResponse(success=False)
            
            # I'm primary, replicate to all backups
            self._replicate_write_to_backups('PlaceBid', request)
            
            with self.data_lock:
                if request.item_id in self.data:
                    item = self.data[request.item_id]
                    if request.bid_amount > item.get('current_price', 0):
                        item['current_price'] = request.bid_amount
                        item['version'] = item.get('version', 0) + 1
                        return mktplace_pb2.PlaceBidResponse(success=True, new_price=request.bid_amount)
                    else:
                        return mktplace_pb2.PlaceBidResponse(success=False, message="Bid too low")
                else:
                    return mktplace_pb2.PlaceBidResponse(success=False, message="Item not found")
        
        except Exception as e:
            print(f"[Node {self.node_id}] PlaceBid error: {e}")
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return mktplace_pb2.PlaceBidResponse(success=False, message=str(e))
    
    def _replicate_write_to_backups(self, method_name: str, request):
        """Replicate write operation to all backup nodes."""
        success_count = 0
        for backup_id, address in self.replica_addresses.items():
            if backup_id == self.node_id:
                continue
            
            try:
                with grpc.insecure_channel(address) as channel:
                    stub = mktplace_pb2_grpc.MarketplaceServiceStub(channel)
                    method = getattr(stub, method_name)
                    response = method(request, timeout=5)
                    success_count += 1
            except Exception as e:
                print(f"[Node {self.node_id}] Failed to replicate {method_name} to node {backup_id}: {e}")
        
        if success_count == 0 and len(self.replica_addresses) > 1:
            print(f"[Node {self.node_id}] Warning: Failed to replicate to any backup nodes")
    


def start_storage_node(node_id: int, replica_addresses: Dict[int, str], is_primary: bool = False, 
                       host='0.0.0.0', port=50051):
    """Start the storage node gRPC server."""
    storage_node = StorageNode(node_id, replica_addresses, is_primary)
    storage_node.startup()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mktplace_pb2_grpc.add_MarketplaceServiceServicer_to_server(storage_node, server)
    
    server.add_insecure_port(f'{host}:{port}')
    
    print(f"[Node {node_id}] Storage node starting on {host}:{port} (Primary: {is_primary})")
    server.start()
    
    try:
        while True:
            time.sleep(86400)  # Keep running
    except KeyboardInterrupt:
        storage_node.shutdown()
        server.stop(0)
        print(f"[Node {node_id}] Storage node stopped")


if __name__ == '__main__':
    # For testing and Docker
    import os
    
    node_id = int(os.getenv('NODE_ID', 0))
    is_primary = os.getenv('IS_PRIMARY', 'false').lower() == 'true'
    host = os.getenv('GRPC_HOST', '0.0.0.0')
    port = int(os.getenv('GRPC_PORT', 50051))
    
    # Hardcoded for docker-compose (adjust as needed)
    replica_addresses = {
        0: 'storage-node-0:50051',
        1: 'storage-node-1:50051',
        2: 'storage-node-2:50051'
    }
    
    start_storage_node(node_id, replica_addresses, is_primary, host=host, port=port)
