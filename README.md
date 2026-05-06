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

