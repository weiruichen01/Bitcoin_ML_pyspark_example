import json
import urllib.request
response = urllib.request.urlopen('https://api.gdax.com/products/BTC-USD/candles?start=2018-01-01T00:00:00Z&end=2018-01-01T01:00:00Z&granularity=60')
json_file = response.read()
a_list = json.loads(json_file)
print(a_list)
print(len(a_list))
