from datetime import datetime

class Task:
    def __init__(self, name, script_path, deadline, est_runtime, args=None, deadline_str=None):
        self.name = name
        self.script_path = script_path
        self.deadline = deadline
        self.est_runtime = est_runtime
        self.args = args or []
        self.enqueued_time = None
        self.deadline_str = deadline_str  # Raw human-readable deadline string (optional)


    def __repr__(self):
        return (f"Task(name={self.name}, deadline={self.deadline}, "
                f"est_runtime={self.est_runtime})")
