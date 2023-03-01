import pandas as pd
from utils import connect_to_db, relative_path

conn = connect_to_db(relative_path("config.yaml"), "database_mongodb", True, "ssh")

print(conn)

database = conn["MOOC"]

collection = database["User"]

df = collection.find_pandas_all({})

print(df)