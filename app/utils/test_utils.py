import time
import functools


def time_it(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = (end_time - start_time) * 1000  # ms
        print(f"함수 {func.__name__} 실행 시간: {execution_time:.2f}ms")
        return result

    return wrapper
