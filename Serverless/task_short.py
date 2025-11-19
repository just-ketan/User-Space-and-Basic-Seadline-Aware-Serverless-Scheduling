import time
from log_util import log_event

def run():
    start = time.time()
    log_event("Short Task Started")
    time.sleep(1)
    end = time.time()
    log_event(f"Short Task Ended, Duration: {end - start:.2f} s")

if __name__ == "__main__":
    run()
