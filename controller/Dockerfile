FROM python:3.9-slim

WORKDIR /app

# Copy requirements
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project
COPY . .

# Controller environment variables
ENV GRPC_HOST=0.0.0.0
ENV GRPC_PORT=50051
ENV PYTHONPATH=/app

CMD ["python", "-m", "controller.controller"]
