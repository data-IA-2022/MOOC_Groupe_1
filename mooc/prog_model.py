
import xgboost as xgb
import pandas as pd
from sklearn.decomposition import KernelPCA
from sklearn.discriminant_analysis import (LinearDiscriminantAnalysis,
                                           QuadraticDiscriminantAnalysis)
from sklearn.ensemble import (AdaBoostClassifier, GradientBoostingClassifier,
                              RandomForestClassifier)
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.pipeline import Pipeline
from sklearn.svm import SVC
from utils import calc_time, connect_ssh_tunnel, connect_to_db, relative_path
from sqlalchemy import text

config_file = relative_path("config_vm.yaml")
sshtunnel_mysql = connect_ssh_tunnel(config_file, "ssh_mysql")

mysqlEngine = connect_to_db(config_file, "database_mysql")

mysqlConn = mysqlEngine.connect().execution_options(isolation_level="AUTOCOMMIT")
mysqlConn.begin()

models = {
    "logistic": LogisticRegression(n_jobs=-1),
    "lda": LinearDiscriminantAnalysis(),
    "adaboost": AdaBoostClassifier(),
    #"gradient-boosting": GradientBoostingClassifier(),
    "rf": RandomForestClassifier(n_estimators=100, n_jobs=-1),
    "knn": KNeighborsClassifier(n_jobs=-1),
    "linear-svc": SVC(kernel="linear"),
    "quadratic-svc": SVC(kernel="poly", degree=2),
    "cubic-svc": SVC(kernel="poly", degree=3),
    "rbf-svc": SVC(kernel="rbf"),
    "xgboost": xgb.XGBClassifier(tree_method="hist", n_jobs=-1, enable_categorical=True)
}

df_messages = pd.read_sql(text("SELECT username, polarity, subjectivity FROM Messages"), mysqlConn)
df_users = pd.read_sql(text("SELECT username, gender, year_of_birth, city, country, level_of_education FROM Users"), mysqlConn)
df_notes = pd.read_sql(text("SELECT username, certificate_delivered FROM Notes"), mysqlConn)

df = df_messages.join((df_users, df_users), 'username', 'inner')

print(df.shape)
print(df.columns)

X_train, X_test, y_train, y_test = train_test_split(X, y_labels, test_size=0.2)
X_train.shape, X_test.shape