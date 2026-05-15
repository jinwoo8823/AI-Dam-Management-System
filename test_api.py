import os
import requests
from dotenv import load_dotenv

# .env 파일 불러오기
load_dotenv()

# .env 안의 API_KEY 값 가져오기
API_KEY = os.getenv("API_KEY")

if not API_KEY:
    raise ValueError(".env 파일에 API_KEY가 없습니다.")

url = "http://apis.data.go.kr/B500001/dam/multipurPoseDam/multipurPoseDamlist"

params = {
    "serviceKey": API_KEY,
    "_type": "json",
    "pageNo": 1,
    "numOfRows": 100,

    # 필수/조회 요청변수
    "tdate": "2026-05-01",  # 선택날짜 기준 전일
    "ldate": "2025-05-02",  # 선택날짜 기준 전년도 날짜
    "vdate": "2026-05-02",  # 선택날짜
    "vtime": "07",          # 조회시간
}

response = requests.get(url, params=params)

print("status_code:", response.status_code)
print("request_url:", response.url)

# JSON 파싱
data = response.json()

items = data["response"]["body"]["items"]["item"]

items = data["response"]["body"]["items"]["item"]

print("\nDam data:")

for dam in items:
    print(
        dam["damnm"],
        dam["nowrsvwtqy"],
        dam["rsvwtrt"]
    )


print(f"\n총 댐 개수: {len(items)}")

print("\n댐 데이터:")
for dam in items:
    print(
        dam["damnm"], 
        dam["nowrsvwtqy"],   # 현재 저수량
        dam["rsvwtrt"]       # 저수율
    )