def encode(latitude: float, longitude: float, precision: int = 12) -> str:
    """
    Encode Latitude and Longitude with precision by Monroe algorithm.

    :param latitude: Latitude of object
    :param longitude: Longitude of object
    :param precision: Precision of GeoHash
    :return: GeoHash of object
    """

    # Raise exception in case of invalid input parameters
    latitude = float(latitude)
    longitude = float(longitude)
    if abs(latitude) > 180 or abs(longitude) > 180:
        raise ValueError("Out of range")

    # Init stage
    __base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
    lat_interval, lon_interval = (-90.0, 90.0), (-180.0, 180.0)
    geohash = []
    bits = [16, 8, 4, 2, 1]
    bit = 0
    ch = 0
    even = True

    # Coding 1 by 1 symbol for geohash until precision is got
    while len(geohash) < precision:
        if even:
            mid = (lon_interval[0] + lon_interval[1]) / 2
            if longitude > mid:
                ch |= bits[bit]
                lon_interval = (mid, lon_interval[1])
            else:
                lon_interval = (lon_interval[0], mid)
        else:
            mid = (lat_interval[0] + lat_interval[1]) / 2
            if latitude > mid:
                ch |= bits[bit]
                lat_interval = (mid, lat_interval[1])
            else:
                lat_interval = (lat_interval[0], mid)
        even = not even
        if bit < 4:
            bit += 1
        else:
            geohash += __base32[ch]
            bit = 0
            ch = 0
    return "".join(geohash)
