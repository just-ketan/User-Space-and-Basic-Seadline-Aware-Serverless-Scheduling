import time

def run():
    start = time.time()
    print(f"Long Task Started at {start}")
    time.sleep(5)   # Simulate longer processing (5 seconds)
    end = time.time()
    print(f"Long Task Ended at {end}, Duration: {end - start:.2f} s")

if __name__ == "__main__":
    run()
