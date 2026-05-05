"""Health monitoring and primary election for storage nodes."""

import grpc
import threading
import time
from typing import Optional

try:
    from gRPC import storage_replica_pb2
    from gRPC import storage_replica_pb2_grpc
except ImportError:
    print("Warning: storage_replica_pb2 not found. See README.md for instructions on generating gRPC code.")

from .registries import StorageNodeRegistry, ServiceNodeRegistry


class HealthMonitor(threading.Thread):
    """Background thread that monitors storage node health via heartbeats and manages primary election."""

    def __init__(self, storage_registry: StorageNodeRegistry, 
                 service_registry: ServiceNodeRegistry, heartbeat_interval: int = 5):
        super().__init__(daemon=True)
        self.storage_registry = storage_registry
        self.service_registry = service_registry
        self.heartbeat_interval = heartbeat_interval
        self.running = True
    
    def run(self):
        """Periodically check health of all nodes."""
        while self.running:
            time.sleep(self.heartbeat_interval)
            self._send_heartbeats()
            self._check_primary_health()
    
    def _send_heartbeats(self):
        """Send heartbeat to all storage nodes."""
        nodes = self.storage_registry.get_all_nodes()
        
        for node_id, node_info in nodes.items():
            address = node_info['address']
            try:
                with grpc.insecure_channel(address) as channel:
                    stub = storage_replica_pb2_grpc.StorageReplicaStub(channel)
                    response = stub.Heartbeat(
                        storage_replica_pb2.HeartbeatRequest(
                            node_id=node_id,
                            is_primary=node_id == self.storage_registry.get_primary()
                        ),
                        timeout=2
                    )
                    self.storage_registry.mark_healthy(node_id, True)
            except Exception as e:
                print(f"[Controller] Heartbeat to storage node {node_id} failed: {e}")
                self.storage_registry.mark_healthy(node_id, False)
                
                # If primary failed, redistribute its service nodes
                if node_id == self.storage_registry.get_primary():
                    print(f"[Controller] Primary node {node_id} is down! Initiating failover...")
                    self._run_failover()
    
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
        print(f"[Controller] Primary node {primary_id} is unhealthy. Electing new primary...")
        self._elect_primary()
    
    def _elect_primary(self):
        """Elect a new primary from healthy nodes."""
        new_primary = self.storage_registry.elect_new_primary()
        
        if new_primary is None:
            print("[Controller] ERROR: No healthy nodes available for primary election!")
            return
        
        print(f"[Controller] Elected node {new_primary} as new primary")
        self._announce_primary(new_primary)
    
    def _run_failover(self):
        """
        Handle primary failure:
        1. Elect a new primary
        2. Redistribute service nodes
        """
        failed_primary = self.storage_registry.get_primary()
        
        # Elect new primary
        self._elect_primary()
        
        # Redistribute service nodes that were attached to failed primary
        self.service_registry.redistribute_services(failed_primary)
    
    def _announce_primary(self, primary_node_id: int):
        """Announce the new primary to all storage nodes."""
        nodes = self.storage_registry.get_all_nodes()
        
        for node_id, node_info in nodes.items():
            address = node_info['address']
            try:
                with grpc.insecure_channel(address) as channel:
                    stub = storage_replica_pb2_grpc.StorageReplicaStub(channel)
                    stub.BecamePrimary(
                        storage_replica_pb2.BecamePrimaryRequest(
                            node_id=primary_node_id
                        ),
                        timeout=2
                    )
            except Exception as e:
                print(f"[Controller] Failed to announce primary to node {node_id}: {e}")
    
    def stop(self):
        """Stop the health monitor."""
        self.running = False
