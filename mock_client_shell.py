#!/usr/bin/env python3
"""
Interactive Mock gRPC CLI Client for Marketplace Controller
Provides an interactive shell to query the marketplace system
"""

import sys
import cmd
import argparse
from typing import List, Optional
import grpc

try:
    from gRPC import mktplace_pb2
    from gRPC import mktplace_pb2_grpc
except ImportError:
    print("Error: Proto files not found. Run: python generate_grpc.py")
    sys.exit(1)


class MarketplaceShell(cmd.Cmd):
    """Interactive shell for marketplace operations."""
    
    intro = """
================================================================
     Mock Marketplace gRPC CLI - Interactive Shell          
   Query the Fault-Tolerant Replicated Marketplace  
================================================================        

Commands:
  create      - Create a new item
  get         - Get item by ID
  search      - Search items
  update      - Update item
  bid         - Place a bid
  auction     - Join an auction
  connect     - Set controller address
  status      - Show current connection
  help        - Show help
  quit/exit   - Exit the shell

Type 'help <command>' for command-specific help.
    """
    
    prompt = 'marketplace> '
    
    def __init__(self, host='localhost', port=50051):
        super().__init__()
        self.host = host
        self.port = port
        self.address = f'{host}:{port}'
    
    def _connect(self):
        """Create a gRPC channel and stub."""
        try:
            channel = grpc.insecure_channel(self.address,
                options=[
                    ('grpc.max_send_message_length', -1),
                    ('grpc.max_receive_message_length', -1)
                ])
            stub = mktplace_pb2_grpc.MarketplaceServiceStub(channel)
            return channel, stub
        except Exception as e:
            print(f"✗ Connection failed to {self.address}: {e}")
            return None, None
    
    def do_connect(self, arg):
        """Connect to a different controller address.
        Usage: connect [host] [port]
        """
        if not arg:
            print(f"Currently connected to: {self.address}")
            return
        
        parts = arg.split()
        if len(parts) >= 1:
            self.host = parts[0]
        if len(parts) >= 2:
            try:
                self.port = int(parts[1])
            except ValueError:
                print("✗ Port must be an integer")
                return
        
        self.address = f'{self.host}:{self.port}'
        print(f"✓ Connected to {self.address}")
    
    def do_status(self, arg):
        """Show current connection status."""
        print(f"Controller Address: {self.address}")
        channel, stub = self._connect()
        if channel:
            print("✓ Connection OK")
            channel.close()
        else:
            print("✗ Connection Failed")
    
    def do_create(self, arg):
        """Create a new marketplace item.
        Usage: create
        You will be prompted for item details.
        """
        try:
            seller = input("Seller ID: ").strip()
            title = input("Item Title: ").strip()
            category = input("Category: ").strip()
            description = input("Description: ").strip()
            price = float(input("Starting Price ($): "))
            quantity = int(input("Quantity: "))
            
            channel, stub = self._connect()
            if not stub:
                return
            
            request = mktplace_pb2.CreateItemRequest(
                seller_id=seller,
                title=title,
                category=category,
                description=description,
                starting_price=price,
                quantity=quantity
            )
            
            response = stub.CreateItem(request, timeout=10)
            
            if response.success:
                print(f"✓ Item created successfully")
                print(f"  Item ID: {response.item_id}")
            else:
                print(f"✗ Failed to create item")
            
            channel.close()
        except ValueError:
            print("✗ Invalid input format")
        except grpc.RpcError as e:
            print(f"✗ gRPC Error: {e.details()}")
        except KeyboardInterrupt:
            print("\n✗ Cancelled")
    
    def do_get(self, arg):
        """Get an item by ID.
        Usage: get <item_id>
        """
        if not arg:
            item_id = input("Item ID: ").strip()
        else:
            item_id = arg.strip()
        
        if not item_id:
            print("✗ Item ID is required")
            return
        
        try:
            channel, stub = self._connect()
            if not stub:
                return
            
            request = mktplace_pb2.GetItemRequest(item_id=item_id)
            response = stub.GetItem(request, timeout=10)
            
            if response.found:
                item = response.item
                print(f"✓ Item found")
                self._print_item(item)
            else:
                print(f"✗ Item not found: {item_id}")
            
            channel.close()
        except grpc.RpcError as e:
            print(f"✗ gRPC Error: {e.details()}")
    
    def do_search(self, arg):
        """Search for items.
        Usage: search [--keyword <keyword>] [--category <category>] [--status <status>]
        """
        try:
            # Simple argument parsing
            keyword = category = status = ''
            parts = arg.split()
            i = 0
            while i < len(parts):
                if parts[i] == '--keyword' and i + 1 < len(parts):
                    keyword = parts[i + 1]
                    i += 2
                elif parts[i] == '--category' and i + 1 < len(parts):
                    category = parts[i + 1]
                    i += 2
                elif parts[i] == '--status' and i + 1 < len(parts):
                    status = parts[i + 1]
                    i += 2
                else:
                    i += 1
            
            channel, stub = self._connect()
            if not stub:
                return
            
            request = mktplace_pb2.SearchItemsRequest(
                keyword=keyword,
                category=category,
                status=status
            )
            
            response = stub.SearchItems(request, timeout=10)
            
            if response.count > 0:
                print(f"✓ Found {response.count} item(s)")
                print("-" * 80)
                for i, item in enumerate(response.items, 1):
                    print(f"\n[Item {i}]")
                    self._print_item(item)
            else:
                print(f"✗ No items found")
            
            channel.close()
        except grpc.RpcError as e:
            print(f"✗ gRPC Error: {e.details()}")
    
    def do_update(self, arg):
        """Update an item.
        Usage: update <item_id>
        You will be prompted for fields to update.
        """
        if not arg:
            item_id = input("Item ID: ").strip()
        else:
            item_id = arg.strip()
        
        if not item_id:
            print("✗ Item ID is required")
            return
        
        try:
            print("Leave fields blank to skip updating them:")
            title = input("New Title [skip]: ").strip()
            description = input("New Description [skip]: ").strip()
            quantity_str = input("New Quantity [skip]: ").strip()
            status = input("New Status [skip]: ").strip()
            price_str = input("New Current Price [skip]: ").strip()
            
            quantity = int(quantity_str) if quantity_str else 0
            current_price = float(price_str) if price_str else 0.0
            
            channel, stub = self._connect()
            if not stub:
                return
            
            request = mktplace_pb2.UpdateItemRequest(
                item_id=item_id,
                title=title,
                description=description,
                quantity=quantity,
                status=status,
                current_price=current_price
            )
            
            response = stub.UpdateItem(request, timeout=10)
            
            if response.overwritten:
                print(f"✓ Item updated (version overwritten)")
            else:
                print(f"✓ Item updated")
            
            channel.close()
        except ValueError:
            print("✗ Invalid input format")
        except grpc.RpcError as e:
            print(f"✗ gRPC Error: {e.details()}")
    
    def do_bid(self, arg):
        """Place a bid on an item.
        Usage: bid
        """
        try:
            item_id = input("Item ID: ").strip()
            bidder_id = input("Bidder ID: ").strip()
            bid_amount = float(input("Bid Amount ($): "))
            
            if not item_id or not bidder_id:
                print("✗ Item ID and Bidder ID are required")
                return
            
            channel, stub = self._connect()
            if not stub:
                return
            
            request = mktplace_pb2.PlaceBidRequest(
                item_id=item_id,
                bidder_id=bidder_id,
                bid_amount=bid_amount
            )
            
            response = stub.PlaceBid(request, timeout=10)
            
            if response.success:
                print(f"✓ Bid placed successfully")
                print(f"  New price: ${response.new_price:.2f}")
            else:
                print(f"✗ Failed to place bid")
            
            channel.close()
        except ValueError:
            print("✗ Invalid input format")
        except grpc.RpcError as e:
            print(f"✗ gRPC Error: {e.details()}")
    
    def do_auction(self, arg):
        """Join an auction with bidirectional streaming.
        Usage: auction
        """
        try:
            item_id = input("Item ID: ").strip()
            bidder_id = input("Bidder ID: ").strip()
            bids_str = input("Bid amounts separated by commas (e.g., 1000,1050,1100): ").strip()
            
            if not item_id or not bidder_id or not bids_str:
                print("✗ All fields are required")
                return
            
            bid_amounts = [float(b.strip()) for b in bids_str.split(',')]
            
            channel, stub = self._connect()
            if not stub:
                return
            
            def bid_generator():
                for bid_amount in bid_amounts:
                    yield mktplace_pb2.AuctionClientMessage(
                        item_id=item_id,
                        bidder_id=bidder_id,
                        bid_amount=bid_amount
                    )
                    print(f"  Sent bid: ${bid_amount:.2f}")
            
            print(f"Joining auction for item {item_id}...")
            responses = stub.JoinAuction(bid_generator(), timeout=30)
            
            message_count = 0
            for response in responses:
                message_count += 1
                item = response.item
                print(f"\n[Server Update {message_count}]")
                print(f"  Item: {item.item_id}")
                print(f"  Current Price: ${item.current_price:.2f}")
                print(f"  Quantity: {item.quantity}")
                print(f"  Status: {item.status}")
                if response.message:
                    print(f"  Message: {response.message}")
                if response.auction_ended:
                    print(f"  [AUCTION ENDED]")
                    break
            
            print(f"\n✓ Auction completed ({message_count} updates)")
            channel.close()
        except ValueError:
            print("✗ Invalid input format")
        except grpc.RpcError as e:
            print(f"✗ gRPC Error: {e.details()}")
    
    def do_quit(self, arg):
        """Exit the shell."""
        print("✓ Goodbye!")
        return True
    
    def do_exit(self, arg):
        """Exit the shell."""
        return self.do_quit(arg)
    
    def do_help(self, arg):
        """Show help."""
        if arg:
            method = 'do_' + arg
            if hasattr(self, method):
                print(getattr(self, method).__doc__)
            else:
                print(f"✗ Unknown command: {arg}")
        else:
            super().do_help(arg)
    
    @staticmethod
    def _print_item(item):
        """Format and print an item."""
        print(f"""  ID:               {item.item_id}
  Seller:           {item.seller_id}
  Title:            {item.title}
  Category:         {item.category}
  Description:      {item.description}
  Starting Price:   ${item.starting_price:.2f}
  Current Price:    ${item.current_price:.2f}
  Quantity:         {item.quantity}
  Status:           {item.status}
  Version:          {item.version}""")


def main():
    parser = argparse.ArgumentParser(description='Marketplace Interactive CLI Shell')
    parser.add_argument('--host', default='localhost', help='Controller host')
    parser.add_argument('--port', type=int, default=50051, help='Controller port')
    
    args = parser.parse_args()
    
    shell = MarketplaceShell(host=args.host, port=args.port)
    shell.cmdloop()


if __name__ == '__main__':
    main()
