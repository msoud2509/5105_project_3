# Replication + Fault Tolerance Plan
A) Fault Tolerance Plan:
1. Failures are detected in the controller either when a failure is returned when trying to forward a request, or when a node fails a health check. The 2 checks here is a complete implementation of failure checking and doesn't really need to be improved.
2. Once a node has been marked as unhealthy, the controller will stop forwarding requests to it. The node will be marked as healthy again if it passes a subsequent health check. If it is still down, we will kill the container and attempt to restart it. Then we will add it back to the registry and see if it is healthy.
3. We will use primary-backup replication, so when a backup fails it is a simple case, we no longer forward requests to it. But when a primary fails, we will promote a backup using leader election.

B) Evaluation Plan:
1. *Performance Metrics*: We will measure latency/throughput, including the worst case of each, to ensure our fault-tolerant strategies do not degrade performance significantly. We will also measure error rate of requests and resource usage of nodes under load.
2. *Handling Multiple Client Testers*: For simplicity, we will run multiple clients in one docker container, using threads to simulate concurrent clients. This allows us to easily scale the number of clients without needing to manage multiple containers.
3. *Test Scaling*: We will ramp up load at a higher rate (again using the multi-threaded client container), making sure that dips in performance are not too much when the system needs to scale up (by adding more replicas). We will also test the system's ability to handle a sudden spike in load, ensuring that it can scale up quickly without crashing.
