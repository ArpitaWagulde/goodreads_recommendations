import pytest

# Tests should run on staging table

def test_normalization():
    data = [100, 200, 300, 400, 500]
    normalized_data = [(x - min(data)) / (max(data) - min(data)) for x in data]
    assert normalized_data == [0.0, 0.25, 0.5, 0.75, 1.0]

if __name__ == "__main__":
    pytest.main()