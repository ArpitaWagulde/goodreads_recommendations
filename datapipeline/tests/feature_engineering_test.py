import pytest

def test_feature_engineering():
    data = [10, 20, 30, 40, 50]
    engineered_data = [x / 10 for x in data]
    assert engineered_data == [1.0, 2.0, 3.0, 4.0, 5.0]

if __name__ == "__main__":
    pytest.main()