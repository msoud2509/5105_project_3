"""Service node that forwards requests to its assigned storage node and manages auction sessions."""

import grpc
import time
import threading
import os
import logging
from concurrent import futures
from typing import Dict, Optional, List

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import generated protobuf classes
try:
    from gRPC import mktplace_pb2
    from gRPC import mktplace_pb2_grpc
    from gRPC import service_node_pb2
    from gRPC import service_node_pb2_grpc
except ImportError:
    logger.warning("Warning: proto files not found. See README.md for instructions on generating gRPC code.")


class ServiceNode(mktplace_pb2_grpc.MarketplaceServiceServicer, 
                  service_node_pb2_grpc.ServiceNodeControlServicer):
    """Service node that forwards requests to assigned storage node and manages auctions."""
    
    def __init__(self, service_node_id: int):
        self.service_node_id = service_node_id
        self.storage_node_address: Optional[str] = None
        self.storage_node_id: Optional[int] = None
        self.lock = threading.Lock()
        
        # Track active auction sessions: {item_id: [list of response queues]}
        self.auction_watchers: Dict[str, List] = {}
        self.auction_lock = threading.Lock()
        
        logger.info(f"[ServiceNode {service_node_id}] Initialized")
    
    ####################### Storage Node Assignment #######################
    
    def AssignStorageNode(self, request, context):
        """Receive storage node assignment from controller."""
        with self.lock:
            self.storage_node_id = request.storage_node_id
            self.storage_node_address = request.storage_node_address
        logger.info(f"[ServiceNode {self.service_node_id}] Assigned to storage node {self.storage_node_id} "
                    f"at {self.storage_node_address}")
        return service_node_pb2.AssignStorageNodeResponse(success=True)
    
    def UpdateStorageNode(self, request, context):
        """Receive storage node update from controller (after failover)."""
        with self.lock:
            old_storage_id = self.storage_node_id
            self.storage_node_id = request.storage_node_id
            self.storage_node_address = request.storage_node_address
        logger.info(f"[ServiceNode {self.service_node_id}] Storage node updated from {old_storage_id} "
                    f"to {self.storage_node_id} at {self.storage_node_address}")
        return service_node_pb2.UpdateStorageNodeResponse(success=True)
    
    # ==================== Storage Node Communication ====================
    
    def _get_storage_stub(self):
        """Get gRPC stub for assigned storage node."""
        with self.lock:
            if not self.storage_node_address:
                raise Exception("Service node not assigned to a storage node yet")
            address = self.storage_node_address
        
        channel = grpc.insecure_channel(address)
        return mktplace_pb2_grpc.MarketplaceServiceStub(channel)
    
    # ==================== Marketplace Service RPCs ====================
    
    def CreateItem(self, request, context):
        """Forward CreateItem to storage node."""
        try:
            stub = self._get_storage_stub()
            response = stub.CreateItem(request, timeout=10)
            return response
        except Exception as e:
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return mktplace_pb2.CreateItemResponse()
    
    def GetItem(self, request, context):
        """Forward GetItem to storage node."""
        try:
            stub = self._get_storage_stub()
            response = stub.GetItem(request, timeout=10)
            return response
        except Exception as e:
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return mktplace_pb2.GetItemResponse()
    
    def SearchItems(self, request, context):
        """Forward SearchItems to storage node."""
        try:
            stub = self._get_storage_stub()
            response = stub.SearchItems(request, timeout=10)
            return response
        except Exception as e:
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return mktplace_pb2.SearchItemsResponse()
    
    def UpdateItem(self, request, context):
        """Forward UpdateItem to storage node."""
        try:
            stub = self._get_storage_stub()
            response = stub.UpdateItem(request, timeout=10)
            return response
        except Exception as e:
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return mktplace_pb2.UpdateItemResponse()
    
    def PlaceBid(self, request, context):
        """Forward PlaceBid to storage node."""
        try:
            stub = self._get_storage_stub()
            response = stub.PlaceBid(request, timeout=10)
            return response
        except Exception as e:
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return mktplace_pb2.PlaceBidResponse()
    
    # ==================== Auction Session Management ====================
    
    def JoinAuction(self, request_iterator, context):
        """
        Bidirectional streaming for auction participation.
        Client sends: item_id to watch, bid amounts to place
        Service node sends: item updates every 2 seconds, auction status
        Service node polls storage node for updates every 2 seconds.
        """
        # Get initial message from client (should contain item_id and bidder_id)
        try:
            first_message = next(request_iterator)
            item_id = first_message.item_id
            bidder_id = first_message.bidder_id
        except StopIteration:
            context.set_details("Client must send initial message with item_id")
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return
        
        logger.info(f"[ServiceNode {self.service_node_id}] Client {bidder_id} joined auction for item {item_id}")
        
        # Create a queue for this client's updates
        client_queue = []
        
        # Register this client as a watcher for this item
        with self.auction_lock:
            if item_id not in self.auction_watchers:
                self.auction_watchers[item_id] = []
            self.auction_watchers[item_id].append(client_queue)
        
        # Start a thread to handle incoming bids from this client
        def handle_client_bids():
            try:
                for message in request_iterator:
                    if message.bid_amount > 0:
                        # Client is placing a bid
                        bid_request = mktplace_pb2.PlaceBidRequest(
                            item_id=item_id,
                            bidder_id=bidder_id,
                            bid_amount=message.bid_amount
                        )
                        try:
                            stub = self._get_storage_stub()
                            bid_response = stub.PlaceBid(bid_request, timeout=10)
                            
                            # Send confirmation back to client
                            client_queue.append(mktplace_pb2.AuctionServerMessage(
                                item=None,
                                auction_ended=False,
                                message=f"Bid placed: ${message.bid_amount}"
                            ))
                        except Exception as e:
                            logger.error(f"[ServiceNode {self.service_node_id}] Error placing bid: {e}")
            except Exception as e:
                logger.error(f"[ServiceNode {self.service_node_id}] Error handling client bids: {e}")
        
        # Start the bid handler thread
        bid_thread = threading.Thread(target=handle_client_bids, daemon=True)
        bid_thread.start()
        
        # Main loop: poll storage node every 2 seconds and send updates to client
        try:
            last_version = -1
            poll_count = 0
            
            while True:
                # Poll storage node for item updates
                try:
                    stub = self._get_storage_stub()
                    get_request = mktplace_pb2.GetItemRequest(item_id=item_id)
                    item_response = stub.GetItem(get_request, timeout=10)
                    
                    if item_response.item and item_response.item.version != last_version:
                        # Item has been updated, send to client
                        last_version = item_response.item.version
                        yield mktplace_pb2.AuctionServerMessage(
                            item=item_response.item,
                            auction_ended=(item_response.item.status == "ended"),
                            message="Item updated"
                        )
                        
                        # If auction ended, break out of loop
                        if item_response.item.status == "ended":
                            break
                except Exception as e:
                    print(f"[ServiceNode] Error polling storage: {e}")
                    yield mktplace_pb2.AuctionServerMessage(
                        item=None,
                        auction_ended=False,
                        message=f"Error: {str(e)}"
                    )
                    break
                
                # Send any queued updates for this client
                if client_queue:
                    while client_queue:
                        queued_msg = client_queue.pop(0)
                        yield queued_msg
                
                # Sleep before next poll
                poll_count += 1
                time.sleep(2)
        
        finally:
            # Unregister client from watchers
            with self.auction_lock:
                if item_id in self.auction_watchers:
                    if client_queue in self.auction_watchers[item_id]:
                        self.auction_watchers[item_id].remove(client_queue)
                    
                    # Clean up empty watcher list
                    if not self.auction_watchers[item_id]:
                        del self.auction_watchers[item_id]
            
            print(f"[ServiceNode {self.service_node_id}] Client {bidder_id} left auction for item {item_id}")


def start_service_node(service_node_id: int, host='localhost', port=50052):
    """
    Start a service node server.
    
    Args:
        service_node_id: Unique ID for this service node
        host: Service node host address
        port: Service node port
    """
    service_node = ServiceNode(service_node_id)
    
    # Create server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Register both servicers
    mktplace_pb2_grpc.add_MarketplaceServiceServicer_to_server(service_node, server)
    service_node_pb2_grpc.add_ServiceNodeControlServicer_to_server(service_node, server)
    
    server.add_insecure_port(f'{host}:{port}')
    
    print(f"[ServiceNode {service_node_id}] Starting on {host}:{port}")
    server.start()
    
    try:
        while True:
            time.sleep(86400)  # Keep running
    except KeyboardInterrupt:
        server.stop(0)
        print(f"[ServiceNode {service_node_id}] Stopped")


if __name__ == '__main__':
    import sys
    
    # Get configuration from environment variables or command line arguments
    node_id = int(os.getenv('NODE_ID', '0'))
    host = os.getenv('GRPC_HOST', 'localhost')
    port = int(os.getenv('GRPC_PORT', '50051'))
    
    # Allow command line override
    if len(sys.argv) > 1:
        node_id = int(sys.argv[1])
    if len(sys.argv) > 2:
        port = int(sys.argv[2])
    
    start_service_node(node_id, host=host, port=port)
