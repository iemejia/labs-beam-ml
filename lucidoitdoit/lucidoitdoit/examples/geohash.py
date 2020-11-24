# input = {"id": "id", "airlines": ['airline1'], "date": '01/01/2010', "lat": 40.7772, "lon": -73.9552 }
import json

def encode(latitude, longitude, precision=12):
    """
    Encode a position given in float arguments latitude, longitude to
    a geohash which will have the character count precision.
    """
    __base32 = '0123456789bcdefghjkmnpqrstuvwxyz'
    __decodemap = { }
    lat_interval, lon_interval = (-90.0, 90.0), (-180.0, 180.0)
    geohash = []
    bits = [ 16, 8, 4, 2, 1 ]
    bit = 0
    ch = 0
    even = True
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
    return ''.join(geohash)

output = json.loads("{}")
output['id'] = input['id']
output['airlines'] = input['airlines']
output['date'] = input['date']
output['location'] = encode(input['lat'], input['lon'])
