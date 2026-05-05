"""Registries for managing storage nodes and service nodes."""

from threading import Lock
from typing import Dict, Optional, List, Tuple


class StorageNodeRegistry:
    """Registry to manage storage nodes, their health statuses, and primary designation."""

    def __init__(self):
        self.lock = Lock()
        self.nodes = {}  # {node_id: {'address': str, 'healthy': bool}}
        self.primary_node_id: Optional[int] = None
    
    def add_node(self, node_id: int, address: str):
        with self.lock:
            self.nodes[node_id] = {
                'address': address,
                'healthy': True
            }
    
    def mark_healthy(self, node_id: int, healthy: bool):
        with self.lock:
            if node_id in self.nodes:
                self.nodes[node_id]['healthy'] = healthy
    
    def get_node_address(self, node_id: int) -> Optional[str]:
        with self.lock:
            if node_id in self.nodes:
                return self.nodes[node_id]['address']
        return None
    
    def get_all_nodes(self) -> Dict[int, Dict]:
        with self.lock:
            return dict(self.nodes)
    
    def get_healthy_nodes(self) -> List[int]:
        with self.lock:
            return [nid for nid, info in self.nodes.items() if info['healthy']]
    
    def set_primary(self, node_id: int):
        with self.lock:
            self.primary_node_id = node_id
    
    def get_primary(self) -> Optional[int]:
        with self.lock:
            return self.primary_node_id
    
    def elect_new_primary(self) -> Optional[int]:
        """
        Elect a new primary from healthy nodes.
        Pick the highest ID healthy node as primary (simple algorithm).
        """
        with self.lock:
            healthy = [nid for nid, info in self.nodes.items() if info['healthy']]
            if not healthy:
                return None
            
            new_primary = max(healthy)
            self.primary_node_id = new_primary
            return new_primary


class ServiceNodeRegistry:
    """Registry to manage service nodes and their attached storage nodes."""

    def __init__(self, storage_registry: StorageNodeRegistry):
        self.lock = Lock()
        self.service_nodes = {}  # {service_id: {'address': str, 'storage_node_id': int}}
        self.storage_registry = storage_registry
        self.service_node_counter = 0  # For assigning unique IDs
        self.round_robin_index = 0  # For round-robin assignment
    
    def add_service_node(self, address: str) -> Tuple[int, int]:
        """
        Add a new service node, assign it to a storage node via round-robin.
        Returns: (service_node_id, assigned_storage_node_id)
        """
        with self.lock:
            service_id = self.service_node_counter
            self.service_node_counter += 1
            
            # Get healthy storage nodes
            healthy_storage = self.storage_registry.get_healthy_nodes()
            if not healthy_storage:
                # this should never happen, but in the chance taht all nodes are unhealthy
                healthy_storage = list(self.storage_registry.get_all_nodes().keys())
            
            if not healthy_storage:
                raise ValueError("No storage nodes available")
            
            # Round-robin assign to a storage node
            assigned_storage_id = healthy_storage[self.round_robin_index % len(healthy_storage)]
            self.round_robin_index += 1
            
            self.service_nodes[service_id] = {
                'address': address,
                'storage_node_id': assigned_storage_id
            }
            
            print(f"[Controller] Service node {service_id} attached to storage node {assigned_storage_id}")
            return service_id, assigned_storage_id
    
    def get_service_node_storage(self, service_id: int) -> Optional[int]:
        with self.lock:
            if service_id in self.service_nodes:
                return self.service_nodes[service_id]['storage_node_id']
        return None
    
    def get_services_on_storage(self, storage_node_id: int) -> List[int]:
        with self.lock:
            return [sid for sid, info in self.service_nodes.items() 
                   if info['storage_node_id'] == storage_node_id]
    
    def redistribute_services(self, failed_storage_id: int):
        """
        rerdistribute service nodes from a failed storage node to other healthy nodes.
        uses round-robin to spread the load.
        """
        with self.lock:
            # get services on failed node
            services = [sid for sid, info in self.service_nodes.items() 
                       if info['storage_node_id'] == failed_storage_id]
            
            if not services:
                return
            
            # get healthy storage nodes (except the failed one)
            healthy_storage = [nid for nid in self.storage_registry.get_healthy_nodes() 
                              if nid != failed_storage_id]
            
            if not healthy_storage:
                print(f"[Controller] Warning: No healthy storage nodes to redistribute to")
                return
            
            for i, service_id in enumerate(services):
                new_storage_id = healthy_storage[i % len(healthy_storage)]
                self.service_nodes[service_id]['storage_node_id'] = new_storage_id
                print(f"[Controller] Service node {service_id} reassigned from "
                      f"storage {failed_storage_id} to storage {new_storage_id}")
    
    def get_all_service_nodes(self) -> Dict[int, Dict]:
        with self.lock:
            return dict(self.service_nodes)
