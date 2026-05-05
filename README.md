# CSCI 5105 Project 3: Fault-Tolerant, Replicated Marketplace with Autoscaling


## Setup Instructions
1. First, enter the devcontainer.
2. Generate gRPC code from the `.proto` files:
```bash
python -m grpc_tools.protoc -IgRPC --python_out=gRPC --grpc_python_out=gRPC gRPC/mktplace.proto gRPC/storage_replica.proto
```
