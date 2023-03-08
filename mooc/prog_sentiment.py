import json

import pandas as pd
import yaml
from pymongo import MongoClient
from sklearn.feature_extraction.text import CountVectorizer
from sqlalchemy import create_engine
from textblob import Blobber
from textblob_fr import PatternAnalyzer, PatternTagger
from utils import calc_time, connect_ssh_tunnel, connect_to_db, relative_path

config_file = relative_path("config.yaml")
sshtunnel_mysql = connect_ssh_tunnel(config_file, "ssh_mysql")

mysqlEngine = connect_to_db(config_file, "database_mysql")

mysqlConn = mysqlEngine.connect().execution_options(isolation_level="AUTOCOMMIT")
mysqlConn.begin()

tb = Blobber(pos_tagger=PatternTagger(), analyzer=PatternAnalyzer())


df=pd.read_sql("SELECT id,body FROM Messages", mysqlConn)

corpus=df.iloc[:,1].values
#print(corpus)

vectorizer = CountVectorizer(strip_accents='ascii')
X = vectorizer.fit_transform(corpus)
names=vectorizer.get_feature_names_out()
#print(*names, 'N=', len(names))

print(X.shape)


quit()
#print(df)

for (i, row) in df.iterrows():
    if i>10: quit()
    #print(i, x)
    blob = tb(row['body'])
    print(row['id'], blob.sentiment)
    #mySQLengine.execute("UPDATE Messages SET polarity=%s, subjectivity=%s WHERE id=%s ;", [blob.sentiment[0], blob.sentiment[1], row['id']])
