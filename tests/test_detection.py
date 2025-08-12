from detection.spike import zscore_spike

def test_zscore_basic():
    series = [10]*29 + [50]
    spike, z = zscore_spike(series, window=30, threshold=2.0)
    assert spike
