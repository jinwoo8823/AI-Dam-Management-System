import requests
from DataBase import DB

api_key = '757a62c8a1dfb5bd5263d612526128493ef53e969f3218144cbe154edf68af38'


class Dam_API:
    def __init__(self):
        self.db = DB()
    
    def Get_Dam_Code(self):    
        url = 'http://apis.data.go.kr/B500001/dam/damCode/damCodelist'
        params ={'serviceKey' : api_key, '_type' : 'json' }

        response = requests.get(url, params=params)

        if response.status_code == 200:
            # 1. JSON 데이터를 파이썬 딕셔너리로 변환
            data = response.json()
            
            # 2. 계층을 따라 'item' 리스트 추출
            # 경로: response -> body -> items -> item
            try:
                dam_list = data['response']['body']['items']['item']
                
                print(f"{'댐코드':<10} | {'댐 이름'}")
                print("-" * 25)
                
                # 3. 반복문으로 코드와 이름만 쏙쏙 뽑기
                for item in dam_list:
                    code = item.get('damcode')
                    name = item.get('damnm')
                    print(f"{code:<10} | {name}")
                    self.db.insert_dam_code(code, name)
            except (KeyError, TypeError) as e:
                print(f"데이터 구조가 예상과 다릅니다: {e}")
        else:
            print(f"요청 실패: {response.status_code}")
            
            
    def Get_Dam_Data(self, dam_code):
            
# dam_api = Dam_API()
# dam_api.Get_Dam_Code()