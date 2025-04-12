import psutil
import time

def monitor_usage(func):
    async def wrapper(*args, **kwargs):
        process = psutil.Process()  # Get the current process
        initial_memory = process.memory_info().rss / (1024 * 1024)  # Initial memory in MB
        initial_cpu = process.cpu_percent(interval=None)  # Initial CPU usage

        print(f"Initial Memory Usage: {initial_memory:.2f} MB")
        print(f"Initial CPU Usage: {initial_cpu:.2f} %")

        start_time = time.time()  # Start timer

        # Track memory usage during function execution
        def track_memory_usage(step):
            memory_usage = process.memory_info().rss / (1024 * 1024)  # Convert to MB
            print(f"{step} - Memory Usage: {memory_usage:.2f} MB")

        track_memory_usage("Before executing query")

        result = await func(*args, **kwargs)  # Execute async function

        track_memory_usage("After executing query")
        
        # Final memory after processing
        end_time = time.time()  # End timer
        final_cpu = process.cpu_percent(interval=None)  # Final CPU usage
        final_memory = process.memory_info().rss / (1024 * 1024)  # Final memory in MB
        
        print(f"Final Memory Usage: {final_memory:.2f} MB")
        print(f"Final CPU Usage: {final_cpu:.2f} %")
        print(f"Execution Time: {end_time - start_time:.2f} seconds")

        return result  # Return the function result

    return wrapper
