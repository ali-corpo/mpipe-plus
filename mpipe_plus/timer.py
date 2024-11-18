import multiprocessing
import threading
import time


class Timer:

    def __init__(self, name,disable=False,per_item=False):
        self.disable = disable
        self.name = name
        self.elapsed_time =0.0
        self.count = 0
        self.per_item = per_item

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.add_from_time(self.start_time)
        
    def add_from_time(self, from_time):
        self.count += 1
        self.elapsed_time += time.time()-from_time    
    def __str__(self):
        if self.disable:
            return ""
        # Convert to hours, minutes, seconds

        per_item_time = self.elapsed_time/self.count if self.per_item else self.elapsed_time
        hours, remainder = divmod(per_item_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        # Build the formatted output conditionally
        time_parts = []
        if hours >= 1:
            time_parts.append(f"{int(hours)}h")
        if minutes >= 1 or hours >= 1:  # Include minutes if there are hours
            time_parts.append(f"{int(minutes)}m")
        time_parts.append(f"{seconds:.2f}s")
        
        # Join the time parts and print
        formatted_time = " ".join(time_parts)
        if not self.per_item:
            return f'{self.name}: {formatted_time}'
        return f'{self.name}: {formatted_time} * {self.count}'
        