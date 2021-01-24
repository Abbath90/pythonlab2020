import pytest

from hotel_analyzer.hotel_analyzer.monroe import encode


class TestMonroe:
    def test_encode_invalid_args(self):
        with pytest.raises(ValueError) as exc:
            encode("aa", 1)
        assert "could not convert" in str(exc.value)

    def test_encode_default_precision(self):
        geohash = encode(-12, 12)
        assert geohash == "kmbe9p6numbe"

    def test_encode_small_precision(self):
        geohash = encode(-12, 12, 5)
        assert geohash == "kmbe9"

    def test_encode_out_of_range(self):
        with pytest.raises(ValueError) as exc:
            geohash = encode(-120000, 1000002, 5)
        assert "Out of range" in str(exc.value)
