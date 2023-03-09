import pickle
from os.path import exists

import optuna
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import f1_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder, MinMaxScaler, OneHotEncoder, RobustScaler
from xgboost import XGBClassifier

from utils import relative_path


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


def get_objective(X_train, X_test, y_train, y_test):

    def objective(trial: optuna.trial.Trial) -> float:

        param = {
            "tree_method": "hist", # trial.suggest_categorical("tree_method", ["gpu_hist", "exact"]),
            # L2 regularization weight.
            "lambda": trial.suggest_float("lambda", 1e-3, 1.0, log=True),
            # L1 regularization weight.
            "alpha": trial.suggest_float("alpha", 1e-8, 1.0, log=True),
            # sampling ratio for training data.
            "subsample": trial.suggest_float("subsample", 0.2, 1.0, log=True),
            # sampling according to each tree.
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.2, 1.0, log=True),
            "n_estimators": trial.suggest_int("n_estimators", 100, 2000),
            "learning_rate": trial.suggest_float("learning_rate", 5e-3, 5e-1, log=True),
            "gamma": trial.suggest_float("gamma", 1e-8, 10.0, log=True),
            "min_child_weight": trial.suggest_int("min_child_weight", 10, 1000, log=True),
        }

        clf = XGBClassifier(n_jobs=-1, verbosity=0, **param)

        pipeline = get_pipeline_model(clf)

        pipeline.fit(X_train, y_train)
        y_pred = pipeline.predict(X_test)

        f1 = f1_score(y_test, y_pred, average="weighted")

        return f1

    return objective


def main():

    df = pd.read_csv(relative_path('dataset_model.csv'))

    X = df.drop(columns=('certificate_delivered'))

    y = df['certificate_delivered']

    y = LabelEncoder().fit_transform(y)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

    if not exists(file_path := relative_path('GIGAMODEL.study')):

        study = optuna.create_study(direction="maximize")  # Create a new study.
        study.optimize(get_objective(X_train, X_test, y_train, y_test), n_trials=500, show_progress_bar=True)  # Invoke optimization of the objective function.

        pickle.dump(study, open(file_path, 'wb'))

    else:

        study = pickle.load(open(file_path, 'rb'))

    clf = XGBClassifier(n_jobs=-1, verbosity=0, **study.best_params)

    pipeline = get_pipeline_model(clf)

    pipeline.fit(X, y)

    pickle.dump(pipeline, open(relative_path('GIGAMODEL.model'), 'wb'))


if __name__ == '__main__':
    main()