#!/usr/local/bin/python3

import pymysql.cursors
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder
from imblearn.over_sampling import SMOTE
from imblearn.pipeline import Pipeline as IMBPipeline
from sklearn.model_selection import RandomizedSearchCV
from sklearn.metrics import classification_report

def get_db_connection():
    connection = pymysql.connect(host='35.224.39.252',
                                user='root',
                                password='12345',
                                db='homework5',
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)
    return connection

def fetch_data():
    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            # Adjust this query to match the specific fields you need
            sql = "SELECT country, age, income, gender FROM request_table;"
            cursor.execute(sql)
            data = cursor.fetchall()
            print("collected the data")
    return data


def train_country_classifier(data):
    df = pd.DataFrame(data)

    # Separate the features and target variable
    X = df.drop('income', axis=1)  # Assuming 'income' is the target variable
    y = df['income']  # This should not be one-hot encoded for classification

    # Apply OneHotEncoder to the categorical features except the target variable 'income'
    categorical_features = ['age', 'country', 'gender']
    onehotencoder = OneHotEncoder(sparse_output=False)
    X_encoded = onehotencoder.fit_transform(X[categorical_features])
    feature_names = onehotencoder.get_feature_names_out(categorical_features)
    
    # Convert the encoded features into a DataFrame
    X_encoded_df = pd.DataFrame(X_encoded, columns=feature_names)

    # Concatenate the original DataFrame (minus the encoded columns) with the new encoded DataFrame
    X = pd.concat([X.drop(categorical_features, axis=1), X_encoded_df], axis=1)

    print("Data has been preprocessed")

    # Split the dataset
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    smote = SMOTE()

    # Expanded parameter grid for hyperparameter optimization
    param_grid = {
        'classifier__n_estimators': [100, 200, 300, 400],
        'classifier__max_depth': [10, 20, 30, 40],
        'classifier__min_samples_split': [2, 5, 10],
        'classifier__min_samples_leaf': [1, 2, 4]
    }

    # Initialize the pipeline
    pipeline = IMBPipeline([
        ('smote', smote),
        ('classifier', RandomForestClassifier(random_state=42, n_jobs=-1))
    ])

    # Initialize RandomizedSearchCV
    random_search = RandomizedSearchCV(pipeline, param_grid, n_iter=100, cv=5, verbose=2, n_jobs=-1, random_state=42)

    # Fit RandomizedSearchCV to the training data
    random_search.fit(X_train, y_train)
    best_clf = random_search.best_estimator_

    # Make predictions on the test set
    y_pred = best_clf.predict(X_test)

    # Evaluate the classifier with additional metrics
    accuracy = accuracy_score(y_test, y_pred)
    print(f"Model accuracy: {accuracy:.2f}")
    print(classification_report(y_test, y_pred))

    # Feature importances (Make sure to extract the classifier if it's inside a pipeline)
    feature_importances = best_clf.named_steps['classifier'].feature_importances_
    important_features = feature_names[feature_importances > np.mean(feature_importances)]
    print(f"Important features based on mean importance: {important_features}")

if __name__ == "__main__":
    data = fetch_data()
    train_country_classifier(data)
