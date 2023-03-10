import pandas as pd
import time
from sqlalchemy import text, update, MetaData
from textblob import Blobber
from textblob_fr import PatternAnalyzer, PatternTagger
from utils import calc_time, connect_ssh_tunnel, connect_to_db, relative_path

global_time = time.time()

config_file = relative_path("config.yaml")
sshtunnel_mysql = connect_ssh_tunnel(config_file, "ssh_mysql")

mysqlEngine = connect_to_db(config_file, "database_mysql")

mysqlConn = mysqlEngine.connect().execution_options(isolation_level="AUTOCOMMIT")
mysqlConn.begin()

META = MetaData()
META.reflect(bind=mysqlEngine)

TABLE_MESSAGES = META.tables['Messages']

tb = Blobber(pos_tagger=PatternTagger(), analyzer=PatternAnalyzer())

df = pd.read_sql(text("SELECT id, body FROM Messages"), mysqlConn)

n_mess = df.shape[0]

n = 0
perc = 0
tout_les_x_perc = 5
t = time.time()

for (i, row) in df.iterrows():

    blob = tb(row['body'])

    stmt = update(TABLE_MESSAGES).where(TABLE_MESSAGES.c.id == row['id']).values(polarity = blob.sentiment[0], subjectivity = blob.sentiment[1])
    mysqlConn.execute(stmt)

    n += 1
    n_perc = int(n / n_mess * 100)

    if perc // tout_les_x_perc != n_perc // tout_les_x_perc:

        perc = n_perc

        print(f" - {perc:3} %   {calc_time(t)}")

        t = time.time()

mysqlConn.commit()

print("FINI", calc_time(global_time))