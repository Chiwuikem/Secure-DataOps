# detection/spike.py
import math

def zscore_spike(series, window=30, threshold=3.0):
    """Return (is_spike, z). series = list[int] trades per second."""
    if len(series) < window:
        return False, 0.0
    s = series[-window:]
    mean = sum(s) / window
    var = sum((x - mean) ** 2 for x in s) / max(window - 1, 1)
    sd = math.sqrt(var) or 1.0
    z = (s[-1] - mean) / sd
    return (z >= threshold), z
