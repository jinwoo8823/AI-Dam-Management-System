import requests
from DataBase import DB
import pandas as pd
import threading as th
import numpy as np
import datetime
import time
from concurrent.futures import ThreadPoolExecutor
import random

api_key = '757a62c8a1dfb5bd5263d612526128493ef53e969f3218144cbe154edf68af38'


class Dam_API:
    def __init__(self):
        self.db = DB()
        self.api_key = api_key
    
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
                    self.db.Insert_Dam_Code(code, name)
            except (KeyError, TypeError) as e:
                print(f"데이터 구조가 예상과 다릅니다: {e}")
        else:
            print(f"요청 실패: {response.status_code}")
            
            
    # 수문운영 정보 가져오기        
    def Get_Dam_Data(self, 수문운영정보_url='http://apis.data.go.kr/B500001/dam/sluicePresentCondition/hourlist'):
        dam_code_list = self.db.Load_Dam_Code()
        data_list = []
        threads = []
    
        # 1. 내부 함수 정의 (함수명을 fetch_data로 변경)
        def fetch_data(code, year):
            page_num = 1
            year_re = f'202{year}'
            
            while True:
                params = {
                    'serviceKey': self.api_key,
                    'pageNo': page_num,
                    'numOfRows': '500',
                    'damcode': code,
                    'stdt': f'{year_re}-01-01',
                    'eddt': f'{year_re}-12-31',
                    '_type': 'json'
                }
                
                try:
                    # API 호출 간격 조절 (지터 추가)
                    time.sleep(1.0) 
                    
                    response = requests.get(수문운영정보_url, params=params, timeout=20)
                    
                    # 'Too Many Requests' 혹은 서버 에러 대응
                    if response.status_code != 200:
                        if response.status_code == 429:
                            print("🚨 트래픽 초과! 5초간 완전히 정지합니다.")
                            time.sleep(5) # 429일 때는 좀 더 길게 쉬어야 합니다.
                        elif response.status_code == 500:
                            print("🔥 서버 내부 오류! 잠시 후 다시 시도합니다.")
                            time.sleep(2)
                        continue

                    response.raise_for_status()
                    res_json = response.json()
                    
                    # [안전장치] 데이터 구조 확인 (string indices 에러 방지)
                    body = res_json.get('response', {}).get('body', {})
                    items_wrapper = body.get('items', {})
                    
                    # 데이터가 없거나 문자열로 올 경우 처리
                    if not isinstance(items_wrapper, dict) or 'item' not in items_wrapper:
                        print(f"{code} / {year_re}: 데이터 없음 (Page {page_num})")
                        break

                    items = items_wrapper['item']
                    if not items:
                        break

                    df = pd.DataFrame(items)
                    df['obsrdt'] = df['obsrdt'].str.replace('시', ':00')
                    df = self.Handle_time(year_re, df) 
                    df['rf'] = df['rf'].fillna(0)
                    df.insert(0, 'dam_code', code)
                    
                    data_list.append(df)
                    print(f"{code} / {year_re} 처리 완료 ({len(df)} row) - Page {page_num}")
                    
                    if len(items) < 500:
                        break
                    page_num += 1
                    
                except Exception as e:
                    print(f"에러 발생 {code}, {year_re}: {e}")
                    # 에러 발생 시 잠시 대기
                    time.sleep(1)
                    break

        # 2. 쓰레드 생성 및 실행 (순차적 실행 간격 추가)
        for i in range(3, 7): # 2020~2026
            for code in dam_code_list:
                t = th.Thread(target=fetch_data, args=(code, i))
                t.start()
                # 쓰레드 시작 시에도 미세한 간격을 두어 게이트웨이 부하 방지
                time.sleep(0.1)

        # 4. 데이터 통합 및 DB 저장
        if data_list:
            final_df = pd.concat(data_list, ignore_index=True)
            print(f"전체 수집 완료: 총 {len(final_df)} 행")
            self.db.Insert_Dam_Data(final_df)
        else:
            print("수집된 데이터가 없습니다.")
        
        
    # DB가 24시00분 지원X -> 00:00으로 변환후 Day + 1로 타입 맞추는 함수 
    def Handle_time(self, year, df : pd.DataFrame):
        df['obsrdt'] = year + '-' + df['obsrdt'].str.strip()
        is_24h = df['obsrdt'].str.contains('24:00')
        df['obsrdt'] = df['obsrdt'].str.replace('24:00', '00:00')
        df['obsrdt'] = pd.to_datetime(df['obsrdt'], format='%Y-%m-%d %H:%M')
        df.loc[is_24h, 'obsrdt'] += pd.Timedelta(days=1)

        return df
    
    
        

        # lowlevel
        # rf
        # inflowqy
        # totdcwtrqy
        # rsvwtqy
        # rsvwtrt

        # response = requests.get(수문운영정보_url, params=params)
        # items = response.json()['response']['body']['items']['item']
        # df = pd.DataFrame(items)
        # print(df)
        
            
# dam_api = Dam_API()
# dam_api.Get_Dam_Data(2022510)