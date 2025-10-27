import pytest

def test_data_cleaning():
    data = [1, 2, None, 4, None, 6]
    cleaned_data = [x for x in data if x is not None]
    assert cleaned_data == [1, 2, 4, 6]

if __name__ == "__main__":
    pytest.main()