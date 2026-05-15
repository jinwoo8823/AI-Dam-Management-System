from sqlalchemy import create_engine
import pandas as pd

engine = create_engine(
    "mysql+pymysql://root:Alexjw750412!@localhost:3306/ai_dam_management"
)

df = pd.read_sql("SELECT * FROM dam_info", engine)
print(df)