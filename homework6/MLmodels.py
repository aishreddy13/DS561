#!/usr/local/bin/python3

import pymysql.cursors
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import joblib
from sklearn.metrics import accuracy_score
from sklearn.metrics import classification_report, confusion_matrix

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
            sql = "SELECT client_ip, country FROM request_table;"
            cursor.execute(sql)
            data = cursor.fetchall()
            print("collected the data")
    return data

def preprocess_ip(ip_address):
    # Replace this with the appropriate preprocessing logic for your model
    octets = ip_address.split('.')
    octets = [int(octet) for octet in octets]
    return octets

def predict_new_data(model, new_data):
    # Assuming 'new_data' is a list of new IP addresses to predict the countries for
    processed_new_data = [preprocess_ip(ip) for ip in new_data]
    predictions = model.predict(processed_new_data)
    return predictions

def train_country_classifier(data):
    # Assuming 'data' is a list of dictionaries with 'client_ip' and 'country' keys
    df = pd.DataFrame(data)
    df['processed_ip'] = df['client_ip'].apply(preprocess_ip)
    print("data has been preprocessed")

    X = df['processed_ip'].tolist()  # Your feature set
    y = df['country']  # Your target variable

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

    rfc = RandomForestClassifier(n_estimators=100)
    rfc.fit(X_train, y_train)
    print("trained model")

    joblib.dump(rfc, 'country_classifier.joblib')

    predicted = rfc.predict(X_test)
    accuracy = accuracy_score(y_test, predicted)
    print(f"Model accuracy on the test set: {accuracy * 100:.2f}%")
    print(classification_report(y_test, predicted))

    # Print confusion matrix
    print(confusion_matrix(y_test, predicted))

if __name__ == "__main__":
    data = fetch_data()
    train_country_classifier(data)
    # Load the trained model
    trained_model = joblib.load('country_classifier.joblib')

    # New data to test
    new_ip_addresses = ['8.8.8.8', '128.101.101.101', '112.97.193.108']  # Add your IP addresses here
    predictions = predict_new_data(trained_model, new_ip_addresses)

    # Now you can print the predictions or compare them with actual countries if you have this information
    print(predictions)
