FROM python:3.9-slim

WORKDIR /app

# Copy requirements from root
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy service node and gRPC
COPY service_nodes /app/service_nodes
COPY gRPC /app/gRPC

# Default to running service node 0 on port 50051 (will be mapped to 50052)
ENV NODE_ID=0
ENV GRPC_HOST=0.0.0.0
ENV GRPC_PORT=50051
ENV CONTROLLER_HOST=controller
ENV CONTROLLER_PORT=50051
ENV PYTHONPATH=/app

CMD ["python", "-m", "service_nodes.service_node"]
