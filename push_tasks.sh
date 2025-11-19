#!/bin/bash

# Usage: ./push_tasks.sh

# Define tasks to push (JSON format)
tasks='[
  {
    "name": "Task4",
    "script_path": "Serverless/task_short.py",
    "deadline": "2025-10-25T10:00:00",
    "est_runtime": 3,
    "args": []
  },
  {
    "name": "Task5",
    "script_path": "Serverless/task_long.py",
    "deadline": "2025-10-25T12:00:00",
    "est_runtime": 5,
    "args": ["--option", "value"]
  }
]'

# Iterate over tasks and push each
for row in $(echo "${tasks}" | jq -c '.[]'); do
  curl -X POST http://localhost:5000/invoke \
    -H "Content-Type: application/json" \
    -d "${row}"
  echo "" # newline for readability
done
