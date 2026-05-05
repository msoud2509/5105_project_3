FROM python:3.9-slim

WORKDIR /app

# Copy requirements from root
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy the storage node
COPY storage /app/storage
COPY gRPC /app/gRPC

# Create data directory
RUN mkdir -p /app/data

# Storage node environment variables
ENV NODE_ID=0
ENV GRPC_HOST=0.0.0.0
ENV GRPC_PORT=50051
ENV PYTHONPATH=/app

CMD ["python", "-m", "storage.storage_node"]
