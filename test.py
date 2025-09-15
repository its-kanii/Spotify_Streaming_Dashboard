from sqlalchemy import create_engine
import pandas as pd

# Use port 3307 (mapped to container)
engine = create_engine("mysql+pymysql://root:rootpassword@localhost:3307/spotify")

df = pd.read_sql("SELECT * FROM user_streams LIMIT 5", engine)
print(df)

