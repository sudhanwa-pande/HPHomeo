from datetime import datetime

def validate_time_format(time_str: str):
    try:
        datetime.strptime(time_str, "%H:%M")
    except ValueError:
        raise ValueError("Time must be in HH:MM format")

def ranges_overlap(ranges):
    sorted_ranges = sorted(ranges, key=lambda x: x.start)
    for i in range(len(sorted_ranges) - 1):
        if sorted_ranges[i].end > sorted_ranges[i+1].start:
            return True
    return False