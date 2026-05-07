import grpc
import time
import concurrent.futures
from gRPC import mktplace_pb2, mktplace_pb2_grpc
import random
# CONFIGURATION
CONTROLLER_ADDR = 'host.docker.internal:50051'
TEST_ITEM_ID = '8866139874'
NUM_REQUESTS = 10000 
CONCURRENT_WORKERS = 30

def get_item_task(stub):
    start = time.perf_counter()
    try:
        response = stub.GetItem(mktplace_pb2.GetItemRequest(item_id=TEST_ITEM_ID), timeout=2)
        return time.perf_counter() - start, response.found
    except Exception as e:
        print(f"Debug Error: {e}") # ADD THIS LINE
        return None, False

def place_bid_task(stub, amount):
    start = time.perf_counter()
    try:
        bid_amount = amount + random.uniform(1.0, 1000.0)
        req = mktplace_pb2.PlaceBidRequest(item_id=TEST_ITEM_ID, bidder_id="eval-bot", bid_amount=bid_amount)
        response = stub.PlaceBid(req, timeout=5)
        return time.perf_counter() - start, response.success
    except Exception as e:
        print(f"Debug Error: {e}")
        return None, False

def run_benchmark(name, task_fn, *args):
    print(f"\n--- Running {name} ---")
    latencies = []
    success_count = 0
    
    with grpc.insecure_channel(CONTROLLER_ADDR) as channel:
        stub = mktplace_pb2_grpc.MarketplaceServiceStub(channel)
        start_bench = time.perf_counter()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as executor:
            # Pass stub and any additional args to the task function
            futures = [executor.submit(task_fn, stub, *args) for _ in range(NUM_REQUESTS)]
            for future in concurrent.futures.as_completed(futures):
                latency, success = future.result()
                if latency is not None:
                    latencies.append(latency)
                if success:
                    success_count += 1
        
        total_time = time.perf_counter() - start_bench
        
    if latencies:
        avg_latency = (sum(latencies) / len(latencies)) * 1000 # convert to ms
        throughput = len(latencies) / total_time
        print(f"Result: {success_count}/{NUM_REQUESTS} successful")
        print(f"Avg Latency: {avg_latency:.2f} ms")
        print(f"Throughput: {throughput:.2f} req/s")
    else:
        print("Test failed: No successful responses.")

if __name__ == "__main__":
    print("Starting Marketplace Evaluation...")
    
    # READ-HEAVY WORKLOAD (Baseline Performance)
    run_benchmark("Read-Heavy (GetItem)", get_item_task)
    
    # WRITE WORKLOAD (Consistency/Latency)
    run_benchmark("Write-Occasional (PlaceBid)", place_bid_task, 10000.0)

    # 3. SCALABILITY BURST (Trigger Autoscaling)
    # Double the requests and incr9ease the workers to hammer the service tier
    NUM_REQUESTS = 20000
    CONCURRENT_WORKERS = 40
    run_benchmark("High-Demand Burst (Autoscaling Test)", get_item_task)