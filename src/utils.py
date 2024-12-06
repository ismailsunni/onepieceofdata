import time


def timing_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Time spent in {func.__name__}: {elapsed_time:.4f} seconds")
        return result

    return wrapper


if __name__ == "__main__":

    @timing_decorator
    def example_function():
        # Your actual function code goes here
        time.sleep(2)
        print("Function executed")

    # Call the decorated function
    example_function()
