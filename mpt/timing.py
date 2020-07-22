import time


def time_usage(func):
    def timing_wrapper(*args, **kwargs):
        start = time.time()
        value = func(*args, **kwargs)
        end = time.time()
        runtime = end - start
        msg = "{func} took {time} seconds"
        print (msg.format(func=func.__name__, time=runtime))
        return value
    return timing_wrapper
