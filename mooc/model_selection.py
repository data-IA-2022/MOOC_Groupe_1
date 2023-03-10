
import time
from typing import Dict

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.ensemble import AdaBoostClassifier, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder, MinMaxScaler, OneHotEncoder, RobustScaler
from sklearn.svm import SVC
from sqlalchemy import text
from utils import connect_ssh_tunnel, connect_to_db, relative_path
from xgboost import XGBClassifier


def fit_model(model, x_train, x_test, y_train, y_test):
    start = time.time()
    model.fit(x_train, y_train)
    elapsed = time.time() - start
    y_predicted = model.predict(x_test)

    return {
        "time": elapsed,
        "accuracy": accuracy_score(y_test, y_predicted),
        "f1": f1_score(y_test, y_predicted, average="weighted"),
    }


def get_pipeline_preparation():

    columns_OneHot          = ['city', 'course_id', 'gender', 'country', 'level_of_education']
    columns_MinMax          = ['year_of_birth']
    columns_Robust          = ['subjectivity', 'nb_messages']
    columns_pass            = ['polarity']

    transfo_year = Pipeline([
        ('imputer', SimpleImputer()),
        ('encoder', MinMaxScaler())
    ])

    transfo_data = Pipeline([
        ('imputer', SimpleImputer(strategy='constant', fill_value='')),
        ('encoder', OneHotEncoder(handle_unknown='ignore', sparse_output=False))
    ])

    transformers = [
        ('col_OnHot',           transfo_data,       columns_OneHot),
        ('col_MinMax',          transfo_year,       columns_MinMax),
        ('col_Robust',          RobustScaler(),     columns_Robust),
        ('col_pass',           'passthrough',       columns_pass),
    ]

    return ColumnTransformer(transformers)


def get_pipeline_model(model):

    return Pipeline([('preparation', get_pipeline_preparation()), ('model', model)])


config_file = relative_path("config.yaml")
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
    "xgboost": XGBClassifier(tree_method="hist", n_jobs=-1, enable_categorical=True)
}

df_messages = pd.read_sql(text("SELECT username, AVG(polarity) as polarity, AVG(subjectivity) as subjectivity, COUNT(*) as nb_messages FROM Messages GROUP BY username"), mysqlConn).set_index('username')
df_users = pd.read_sql(text("SELECT username, gender, year_of_birth, city, country, level_of_education FROM Users"), mysqlConn).set_index('username')
df_notes = pd.read_sql(text("SELECT username, certificate_delivered, course_id FROM Notes"), mysqlConn).set_index('username')

sshtunnel_mysql.close()

df = df_users.join(df_messages, 'username', 'inner')
df = df.join(df_notes, 'username', 'inner').reset_index()

df.dropna(subset = ['certificate_delivered'], inplace=True)
df.drop(columns='username', inplace=True)

print(df.shape)
print(df.isna().sum())
print(df.head(20))

df.to_csv(relative_path('dataset_model.csv'), index = False)

X = df.drop(columns=('certificate_delivered'))

y = df['certificate_delivered']

y = LabelEncoder().fit_transform(y)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

result: Dict[str, float] = {}

for name, model in models.items():
    print("Training ", name, "...")
    pipeline = get_pipeline_model(model)
    result[name] = fit_model(
        pipeline,
        X_train,
        X_test,
        y_train,
        y_test
    )
    for key, value in result[name].items():
        print(f"\t{key.capitalize()}: {value}")

result = pd.DataFrame(result).T
result.to_csv(relative_path('model_result.csv'))