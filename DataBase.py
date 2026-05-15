# import pymysql
from turtle import up
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import os


class DB:
    def __init__(self):
        self.dam_name = ['소양강', '충주', '횡성', '안동', '임하', '성덕', '영주', '군위', '보현산', '합천', '남강',
                    '밀양', '용담', '대청', '섬진강', '주암(본)', '주암(조)', '부안', '보령', '장흥']
        # postgresql version
        self.db = psycopg2.connect(
            host='127.0.0.1',
            # user는 자기 컴퓨터 계정 아이디로 변경
            # user='postgres',
            # password='tjddus@@1387',
            user='kimsy', # mac에서 실행할때
            password='1234',
            dbname='dam_project'
        )
        self.cur = self.db.cursor()
        
        
        # mysql version
        # self.db = pymysql.connect(
        #     host='127.0.0.1',
        #     user='root',
        #     password='',
        #     database='dam_project',
        #     charset='utf8mb4'
        # )
        # self.cur = self.db.cursor(pymysql.cursors.DictCursor) # mysql version
        
   
   
    # postgresql 버전
    #region 댐코드 저장 한번만 실행
    def Insert_Dam_Code(self, dam_code, dam_name):
        self.cur.execute("INSERT INTO Dam_Code(dam_code, dam_name) VALUES (%s, %s)", (dam_code, dam_name))   
        self.db.commit()
        print(f'{dam_name} 댐 코드 저장 완료')
    
    # 댐 코드 불러오는 함수
    def Load_Dam_Code(self):
        self.cur.execute("SELECT * FROM Dam_Code")
        rows = self.cur.fetchall()
        dam_code_list = []
        dam_all_list = []
        for row in rows:
            dam_code_list.append(row[0])
            dam_all_list.append(row)
            
        return dam_code_list, dam_all_list
    
    
    # 수문운영 정보 저장
    def Insert_Dam_Data(self,  dam_data : pd.DataFrame):
        # dam_data는 dict 타입 또는 순서가 보장된 tuple/list라고 가정 (예: (dam_code, obsrdt, lowlevel, rf, inflowqy, totdcwtrqy, rsvwtqy, rsvwtrt))
        # Dam_Operation_Data 테이블은 dam_code와 obsrdt(날짜)가 복합키, dam_code는 Dam_Code 참조
        insert_query = """
                INSERT INTO dam_operation_data (
                    dam_code, inflowqy, lowlevel, obsrdt, rf, rsvwtqy, rsvwtrt, totdcwtrqy
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (dam_code, obsrdt) DO NOTHING
            """
        
        data = dam_data.to_numpy()
        if data is not None and data.size > 0:
            # 3. 튜플 리스트로 변환 (DB 드라이버가 가장 좋아하는 타입)
            # ndarray를 그대로 넣기보다 튜플로 감싸주는 것이 안전합니다.
            records = [tuple(x) for x in data] 
            try:     
                self.cur.executemany(insert_query, records)
                self.db.commit()
                print(f'{data['dam_code']} 댐 운영정보 데이터 저장 완료')
            except Exception as e:
                print(f'{data[0]}는 API 지원 X\n{e}')
                
                
    # def Dam_X_Y_Insert(self): # 댐 데이터 -> 좌표 저장
    #     dam_info = {
    #         "소양강": [37.9450000, 127.8150000, 74, 135],
    #         "충주": [37.0050000, 127.9850000, 77, 115],
    #         "횡성": [37.5400000, 128.0500000, 78, 126],
    #         "안동": [36.5840000, 128.7700000, 91, 106],
    #         "임하": [36.5400000, 128.8800000, 93, 105],
    #         "성덕": [36.3000000, 129.0000000, 96, 100],
    #         "영주": [36.8000000, 128.6800000, 90, 111],
    #         "군위": [36.1200000, 128.6500000, 90, 96],
    #         "김천부항": [35.9900000, 127.9800000, 78, 93],
    #         "보현산": [36.1400000, 128.9300000, 94, 97],
    #         "합천": [35.5300000, 128.0300000, 79, 83],
    #         "남강": [35.1600000, 128.0400000, 79, 75],
    #         "밀양": [35.5000000, 128.9300000, 95, 83],
    #         "용담": [35.9400000, 127.5300000, 70, 92],
    #         "대청": [36.4775000, 127.4800000, 69, 103],
    #         "섬진강": [35.5400000, 127.1400000, 63, 83],
    #         "주암(본)": [35.0700000, 127.2400000, 65, 72],
    #         "주암(조)": [35.0600000, 127.3100000, 66, 72],
    #         "부안": [35.6900000, 126.6200000, 54, 86],
    #         "보령": [36.3600000, 126.6500000, 54, 100],
    #         "장흥": [34.7600000, 126.9300000, 60, 66]
    #     }
    
    #     for dam_name, values in dam_info.items():
    #         lat, lon, nx, ny = values
    #         sql = """
    #             UPDATE dam_code
    #             SET
    #                 lat = %s,
    #                 lon = %s,
    #                 nx = %s,
    #                 ny = %s
    #             WHERE dam_name = %s
    #         """
    #         self.cur.execute(sql, (
    #             lat,
    #             lon,
    #             nx,
    #             ny,
    #             dam_name
    #         ))
    #         self.db.commit()
    #     self.cur.close()
    
    # 수문 운영 데이터 로드
    def Load_Dam_Data(self):
        cnt = 0
        error = []

        code_list, no = self.Load_Dam_Code()
        expected = None

        for code in code_list:
            query = """
                SELECT *
                FROM dam_operation_data
                WHERE dam_code = %s
                ORDER BY obsrdt;
            """
            df = pd.read_sql(query, self.db, params=(code,))
            current_len = len(df)
            # 첫 번째 댐 기준 저장
            if expected is None:
                expected = current_len
            # 개수 비교
            if current_len != expected:
                error.append({
                    'dam_code': code,
                    'count': current_len
                })
            print(code, current_len)
        print("문제있는 댐:")
        print(error)
        print(len(error))

        self.db.close()
        
    # 기상청 데이터 전처리
    def Final_Data_File(self):
        no, a = self.Load_Dam_Code()
        data_dict_list = {}
        
        
        for d in a:
            if d[2] != None:
                data_dict_list[d[1]] = [d[0]] # dict to list
                
        print(data_dict_list)
        
        for name in self.dam_name:
            file = f'/Users/kimsy/Desktop/프로젝트/파이썬 실습/기말프로젝트/AI-Dam-Management-System/dam_weather_data/{name}댐_1시간강수량_20221231_20260507.csv'
            if not os.path.exists(file):
                print(f"❌ 파일이 존재하지 않습니다: {file}")
                break
            df = pd.read_csv(file)
            
            # print(df.head(10))
            
            col_to_rename = df.columns[3]
            first_col = df.columns[0]
            df = df.rename(columns={col_to_rename: 'value'.strip()})
            df = df.rename(columns={first_col: 'day'.strip()})
       
            day_column = df['day'].astype(str).reset_index(drop=True)
            filled_days = []
            current_date = None

            for i, val in enumerate(day_column):
                if val.strip().startswith('Start :'):
                    # "Start :"가 붙은 행이면, 날짜 추출해서 저장만 하고, 해당 행은 drop 대상이 됨(즉, day로 쓰임)
                    current_date = val.split('Start :', 1)[1].strip()
                    filled_days.append(np.nan)  # 해당 row는 어차피 의미 없는 row이므로 나중에 삭제 용으로 nan 처리
                else:
                    filled_days.append(current_date if current_date is not None else val)

            df['day'] = filled_days

            # "Start :" 행(즉, np.nan으로 바꾼) 제거
            df = df[df['day'].notna()].reset_index(drop=True)
            

            # day, hour, forecast, value 타입을 float->int/str/float로 변경 (확실하게!)
            if 'hour' in df.columns:
                try:
                    df['hour'] = df['hour'].astype(float).astype(int)
                except Exception:
                    pass
            if 'forecast' in df.columns:
                try:
                    df['forecast'] = df['forecast'].astype(float).astype(int)
                except Exception:
                    pass
            try:
                df['day'] = df['day'].astype(int)
            except Exception:
                try:
                    # date형식이 섞인 경우는 그대로 두기 (ex. '20221231')
                    df['day'] = df['day'].astype(str)
                except Exception:
                    pass
            df.insert(0, 'dam_code', data_dict_list[name][0])

            # 변경 결과, day 값 중 "Start :"가 남아있나, 잘못된 값이 있나 1차 검사
            print("고유 day 예시:", df['day'].unique()[:5])

            # 혹시 남은 이상 행
            print("남아있는 'Start :' 행 (있으면 버그):", df[df['day'].astype(str).str.startswith("Start :")].shape[0])
            
            if isinstance(df, pd.DataFrame):
                # 마지막 컬럼, 마지막에서 2번째 컬럼 이름 추출 (컬럼 순서를 모를 수 있으니)
                last_col = df.columns[-1]
                second_last_col = df.columns[-2]
                # value가 0.0이고 두 번째 마지막 컬럼이 25,26,27이면 삭제 아니고, 그 외(즉, 25,26,27이 아닐 때만) 삭제
                # forecast(예측시간) 컬럼 값이 28 미만인 행만 남깁니다.
                
                df = df[df['forecast'] < 28.0]
         
                mask = df['value'] > 0.0
                print(df.columns)
                df_filtered = df[mask].reset_index(drop=True)
            df = df_filtered
            data_dict_list[name].append(df)
        
            print(f'{name} 데이터 전처리 완료')
            print('============= 전처리된 데이터 =============')
            print(data_dict_list[name][1].tail(10))
            
        
        return data_dict_list
         
                
    
    def Final_Data_Join(self):
        data_dict_list_int_DataFrame = self.Final_Data_File()
        new_df : pd.DataFrame
        all_df : pd.DataFrame
        # print(f'전체 데이터\n{data_dict_list_int_DataFrame}')
        for name in self.dam_name:
            df = data_dict_list_int_DataFrame[name][1]
            # print(f'return 받은 데이터 한번 출력\n{df}')
            df['hour'] = df['hour'].astype(int).astype(str).str.zfill(4)

# 발표시간 생성
            df['tmfc'] = pd.to_datetime(
                df['day'].astype(str) + df['hour'],
                format='%Y%m%d%H%M'

            )
            # 예측 대상 시간 생성
            # new_df를 조건에 맞게 생성: value가 1.0이고, forecast, day, hour에 맞는 target_time 계산
            # 예: 2025년01월01일 02시에 예측, forecast=6, value=1.0 -> 2025년01월01일 08시, value=1.0
            # DataFrame의 각 row별로 계산
            cond = (df['value'] > 0.0)
            # 필요한 컬럼만 새로 뽑아서 new_df 생성
            filtered_df = df.loc[cond, ['dam_code', 'day', 'hour', 'forecast', 'value']].copy()
            # hour(문자열)추출, 0패딩 보장, int->str으로 변환
            filtered_df['hour'] = filtered_df['hour'].astype(int).astype(str).str.zfill(4)
            # 발표시간
            filtered_df['tmfc'] = pd.to_datetime(
                filtered_df['day'].astype(str) + filtered_df['hour'],
                format='%Y%m%d%H%M'
            )
            # 예측 대상 시간
            filtered_df['target_time'] = filtered_df['tmfc'] + pd.to_timedelta(filtered_df['forecast'].astype(int), unit='h')
            filtered_df['target_time'] = pd.to_datetime(filtered_df['target_time'])
            # 최종 제출용 DataFrame. 컬럼 이름 맞추기
            new_df = pd.DataFrame({
                'dam_code': filtered_df['dam_code'],
                'timestamp': filtered_df['target_time'],
                'value': filtered_df['value']
            }).reset_index(drop=True)
            if 'all_df' in locals() and isinstance(all_df, pd.DataFrame):
                all_df = pd.concat([all_df, new_df], ignore_index=True)
            else:
                all_df = new_df.copy()
            
        print(all_df)

        
        return all_df

        
        # 올바른 코드 예시 및 주요 오류 원인 주석
    def save_to_database(self, all_df : pd.DataFrame):
        sql = """
                UPDATE dam_operation_data
                SET rain = %s
                WHERE dam_code = %s
                AND obsrdt = %s;
            """

   
        
        if not np.issubdtype(all_df['timestamp'].dtype, np.datetime64):
            try:
                all_df['timestamp'] = pd.to_datetime(all_df['timestamp'])
            except Exception as e:
                print("timestamp 컬럼 datetime 변환 오류:", e)
                return
        all_df = all_df.drop_duplicates(subset=['dam_code', 'timestamp'], keep='first')
        all_df.to_csv('기상청csv파일', index=False, encoding='utf-8-sig')
        all_df = all_df.to_numpy()
        print(all_df)
        update_data = [(rain, dam_code, timestamp) for dam_code, timestamp, rain in all_df]
        update_data = list(set(update_data))
        

        try:
            self.cur.executemany(sql, update_data)
            self.db.commit()
            print(f'데이터 저장 완료 전체 행 -> {len(update_data)}')
        except Exception as e:
            print('기상청 데이터 최종 저장 중 오류 발생:', e)
            self.db.rollback()

    
         
    
    
    def Export_CSV(self):

        # 1. 데이터베이스에서 데이터 불러오기 (예시: DataFrame 생성)
        # 이미 self.db가 커넥션이라고 가정, 혹은 별도 커넥션 사용 가능
        query = "SELECT * FROM dam_operation_data"
        df = pd.read_sql(query, self.db)

        # 2. DataFrame을 CSV 파일로 저장
        output_filename = 'final_data.csv'
        df.to_csv(output_filename, index=False, encoding='utf-8-sig')

        print(f"'{output_filename}' 파일로 저장 완료되었습니다.")
        
        
    def Load_ALL_Data(self):
        _, all_dam_code = self.Load_Dam_Code()  # 코드, 전체데이터
        query = """SELECT *
                FROM dam_operation_data
                WHERE dam_code = %s
                ORDER BY obsrdt ASC;
                """
        code = []
        dam_data_dict_DataFrame = {}

        for name in all_dam_code:
            if name[1] in self.dam_name:
                code.append(name[0])

        for dam_code in code:
            self.cur.execute(query, (dam_code,))
            row = self.cur.fetchall()
            df = pd.DataFrame(row)
            dam_data_dict_DataFrame[dam_code] = df  # dam_code를 key, df를 value로 저장

   
            
        return dam_data_dict_DataFrame, code
    
    
    def Model_Performance_Save(self, data):
        
        query = """
            INSERT INTO model_evaluation 
            dam_code = %s,
            model = %s,
            mae = %s,
            rmse = %s,
            r2 = %s,
            accuracy = %s,
            recommendation = %s
        """
        
        data = data.to_numpy()
        data_tuple = [tuple(i) for i in data]
        
        try:
            self.cur.executemany(query, data_tuple)
            self.db.commit()
        except Exception as e:
            print(f'모델 평가지표 저장중 오류발생 -> {e}')
            
    
            
            
        
        
     
        #     print(df.head(1))
        #     print(df.tail(1))
        #     print(len(df))
        #     length.append(len(df))
        # print(len(length))
        # print(set(length))
            
    
            


if __name__ == "__main__":    
    db = DB()
    # df = db.Final_Data_Join()
    # db.save_to_database(df
    # db.Load_ALL_Data()
    