# import pymysql
import psycopg2
import pandas as pd


class DB:
    def __init__(self):
        
        # postgresql version
        self.db = psycopg2.connect(
            host='127.0.0.1',
            # user는 자기 컴퓨터 계정 아이디로 변경
            user='postgres',
            password='tjddus@@1387',
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
    # def Insert_Dam_Code(self, dam_code, dam_name):
    #     self.cur.execute("INSERT INTO Dam_Code (dam_code, dam_name) VALUES (%s, %s)", (dam_code, dam_name))   
    #     self.db.commit()
    #     print(f'{dam_name} 댐 코드 저장 완료')
    
    # 댐 코드 불러오는 함수
    def Load_Dam_Code(self):
        self.cur.execute("SELECT * FROM Dam_Code")
        rows = self.cur.fetchall()
        dam_code_list = []
        for row in rows:
            dam_code_list.append(row[0])
        return dam_code_list
    
    
    # 수문운영 정보 저장
    def Insert_Dam_Data(self,  dam_data : pd.DataFrame):
        # dam_data는 dict 타입 또는 순서가 보장된 tuple/list라고 가정 (예: (dam_code, obsrdt, lowlevel, rf, inflowqy, totdcwtrqy, rsvwtqy, rsvwtrt))
        # Dam_Operation_Data 테이블은 dam_code와 obsrdt(날짜)가 복합키, dam_code는 Dam_Code 참조
        insert_query = """
                INSERT INTO dam_operation_data (
                    dam_code, obsrdt, lowlevel, rf, inflowqy, totdcwtrqy, rsvwtqy, rsvwtrt
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
            
            
            
            
            
            
            
            
        
        
# db = DB()
# dam_code = db.Load_Dam_Code()
# print(dam_code)
    