FROM python:3.9-slim

# Set the working directory
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m","mock_client.mock_client_shell", "--host", "marketplace-controller", "--port", "50051"]