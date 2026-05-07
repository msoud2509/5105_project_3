# CSCI 5105 Project 3: Fault-Tolerant, Replicated Marketplace with Autoscaling


## Setup Instructions
1. First, enter the devcontainer.
2. Generate gRPC code from the `.proto` files (this also fixes the imports automatically):
```bash
python generate_grpc.py
```
This script will do the normal gRPC code generation, but will manually fix importing issues that came up due to the directory structure.

3. Create the docker system by running the following command in the root directory:
```bash
docker compose up
```
On the safe side, will need to wait around 30 seconds to allow the primary to complete replication of data to the backup nodes.

4. Then run the mock client container in a separate terminal:
```bash
docker compose run --rm mock-client
```

## Testing failover (fault tolerance)
To test failover, you can stop any of the storage nodes by running the following command in a separate terminal:
```bash
docker compose down storage-node-1
```
**NOTE**: You can replace the container name here with any of the storage nodes (`storage-node-0`, `storage-node-2`), `storage-node-2` is the initial primary node.

Then you can check the controller's logs to ensure that failover logic is working correctly (no longer route to that node, and promote backup to primary if the failed node was the primary, and new docker container is created, and data is replicated)

**NOTE**: the new docker containers created as a result are outside of the docker compose scope, so running `docker compose down` will not stop them, you will need to manually stop them.


## Testing reads, writes and autoscaling

The script evaluate_system.py has three tests. Before running the tests make sure to do:
```bash
docker compose up --build
```

Also change the self._cooldown in controller/scaling_manager.py in the init function to 5 for testing.

1. The first test is for reads, in this case it does many get item requests. Comment out the test 2 and 3 in the main part of the function to see the functionality.

2. The second test is for writes, for bids and auctions. Comment out test 1 and 3 to see how this would perform.


3. The last test tests for sudden bursts of requests to test autoscaling. Remember to change the self._cooldown in controller/scaling_manager.py in the init function to 5. 

