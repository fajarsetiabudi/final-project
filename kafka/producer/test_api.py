import requests
import time

# data = []
def get_stream(url):
    s = requests.Session()

    with s.get(url, headers=None, stream=True) as resp:
        for line in resp.iter_lines():
            if line:
                print(line)
                # data.append(line)

url = 'https://www.freeforexapi.com/api/live?pairs=EURUSD,EURGBP,USDEUR'

while True:
    get_stream(url)
    time.sleep(60)
