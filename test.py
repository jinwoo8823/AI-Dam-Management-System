import requests
import pandas as pd
import numpy as np
api_key = '757a62c8a1dfb5bd5263d612526128493ef53e969f3218144cbe154edf68af38'
수문운영정보_url = 'http://apis.data.go.kr/B500001/dam/sluicePresentCondition/hourlist'
params ={'serviceKey' : api_key, 'pageNo' : 1, 'numOfRows' : '500', 'damcode' : '2403201', 'stdt' : '2020-01-31', 'eddt' : '2020-01-31', '_type' : 'json' }


다목적댐_url = 'http://apis.data.go.kr/B500001/dam/multipurPoseDam/multipurPoseDamlist'
다목적_params ={'serviceKey' : api_key, 'pageNo' : '1', 'numOfRows' : '10', 'tdate' : '2018-08-19', 'ldate' : '2017-08-20', 'vdate' : '2018-08-20', 'vtime' : '07', '_type' : 'json' }

response = requests.get(다목적댐_url, params=다목적_params)
items = response.json()['response']['body']['items']['item']
# print(items)
df = pd.DataFrame(items)
print(df.columns)
drop_cols = [
    'dvlpqyacmtlacmslt',   # 발전량 실적
    'dvlpqyacmtlplan',     # 발전량 계획
    'dvlpqyacmtlversus',   # 발전량 계획대비
    'dvlpqyfyerplan',      # 연간발전계획
    'dvlpqyfyerversus',    # 연간계획대비
    'vyacurf',             # 전년 누계강우량
    'lastlowlevel',        # 저수위 전년
    'lastrsvwtqy',         # 저수량 전년
    # 'damnm'                # 댐이름, 표시용이면 제거
]

df = df.drop(drop_cols, axis=1)

num_cols = [
    'pyacurf',
    'oyaacurf',
    'nowrsvwtqy',
    'nyearrsvwtqy',
    'nowlowlevel',
    'nyearlowlevel'
]

for col in num_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')

df['누계강우_편차'] = (df['pyacurf']) - (df['oyaacurf'])
df['저수량_편차'] = (df['nowrsvwtqy']) - (df['nyearrsvwtqy'])
df['저수위_편차'] = (df['nowlowlevel']) - df['nyearlowlevel']
df['누계강우_비율'] = (df['pyacurf']) / df['oyaacurf'].replace(0, np.nan)
df['저수량_비율'] = (df['nowrsvwtqy']) / df['nyearrsvwtqy'].replace(0, np.nan)
print(df)
# 에년 데이터 -> 저수량(nyearrsvwtqy), 저수위(nyearlowlevel), 예년 누계강우량(oyaacurf)

# print(df)

# lowlevel
# rf
# inflowqy
# totdcwtrqy
# rsvwtqy
# rsvwtrt

# a = [1,2,3]
# response = requests.get(수문운영정보_url, params=params)
# # print(response.content)
# items = response.json()['response']['body']['items']['item']
# df = pd.DataFrame(items)
# print(df)
# # dfs = df.to_numpy()

# df['obsrdt'] = df['obsrdt'].str.replace('시', ':00')
# df['obsrdt'] = '2025' + '-' + df['obsrdt'].str.strip()
# is_24h = df['obsrdt'].str.contains('24:00')
# df['obsrdt'] = df['obsrdt'].str.replace('24:00', '00:00')
# df['obsrdt'] = pd.to_datetime(df['obsrdt'], format='%Y-%m-%d %H:%M')
# df.loc[is_24h, 'obsrdt'] += pd.Timedelta(days=1)
# print(len(df))
# df = df.to_numpy()
# print(df[0])





# d = [{'a' : df}, {'b': df}]
# lis = ['c', 'a', 'b']
# for code in lis:
#     for i in d:
#         datas = i.get(code)
#         print(datas)
#         print(1)


# di = {'a': 1, 'b' : 2}
# print(di.get('a'))