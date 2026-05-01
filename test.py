import requests
import pandas as pd

api_key = '757a62c8a1dfb5bd5263d612526128493ef53e969f3218144cbe154edf68af38'
url = 'http://apis.data.go.kr/B500001/dam/sluicePresentCondition/hourlist'
params ={'serviceKey' : api_key, 'numOfRows' : '10', 'damcode' : '2022510', 'stdt' : '2018-10-01', 'eddt' : '2018-10-01', '_type' : 'json' }


# lowlevel
# rf
# inflowqy
# totdcwtrqy
# rsvwtqy
# rsvwtrt


response = requests.get(url, params=params)
print(response.content)

items = response.json()['response']['body']['items']['item']
df = pd.DataFrame(items)
print(df)