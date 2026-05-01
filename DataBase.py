import pymysql


class DB:
    def __init__(self):
        self.db = pymysql.connect(
            host='127.0.0.1',
            user='root',
            password='tjddus@@1387',
            database='dam_project',
            charset='utf8mb4'
        )
        self.cur = self.db.cursor(pymysql.cursors.DictCursor)
        
    def insert_dam_code(self, dam_code, dam_name):
        self.cur.execute("INSERT INTO Dam_Code (dam_code, dam_name) VALUES (%s, %s)", (dam_code, dam_name))
        self.db.commit()
        # print(f'{dam_name} 댐 코드 저장 완료')