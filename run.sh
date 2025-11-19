#!/bin/bash

# Run Flask middleware in background
echo "Starting middleware..."
python3 Scheduler/middleware.py &
MIDDLEWARE_PID=$!

# Wait for middleware to start listening
echo "Waiting for middleware to become active..."
while ! curl -s http://localhost:5000/status > /dev/null; do
  sleep 1
done
echo "Middleware is live."

# Run workload test script to invoke tasks
echo "Running workload invocation script..."
python3 test_invoke_workloads.py

# Optional: Shutdown middleware after testing
echo "Stopping middleware..."
kill $MIDDLEWARE_PID
wait $MIDDLEWARE_PID 2>/dev/null

echo "Done."
