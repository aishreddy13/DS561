#!/usr/local/bin/python3

from http.server import BaseHTTPRequestHandler, HTTPServer
import argparse
import time
import google.cloud.storage as storage
import google.cloud.pubsub as pubsub
import pymysql.cursors
import datetime


class MyServer(BaseHTTPRequestHandler):

    def publish_pub_sub(self, message):
        project_id = 'u62138442-hw2'
        topic_id = project_id + 'topic_1'
        publisher = pubsub.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_id)
        data = message.encode('utf-8')
        future = publisher.publish(topic_path, data)
        future.result()
    
    # def do_GET(self):
    #     country = self.headers['X-Country']
    #     if country in ['North Korea', 'Iran', 'Cuba', 'Myanmar', 'Iraq', 'Libya', 'Sudan', 'Zimbabwe', 'Syria']:
    #         publish_pub_sub('Banned country ' + country)
    #     ip = self.headers['X-Client-IP']
    #     bucket = 'u62138442_hw2_b2'
    #     filename = None
    #     parts = self.path.split('/')
        
    #     if len(parts) < 3:  # Ensure the path has at least 2 slashes (3 components)
    #         # Handle the error, maybe send a 400 Bad Request or a default response
    #         self.send_response(400)
    #         self.send_header("Content-type", "text/html")
    #         self.end_headers()
    #         self.wfile.write(bytes("Bad request.", "utf-8"))
    #         return

    #     bucket = parts[1]
    #     filename = parts[2]            
    #     self.send_gcs_response(bucket, filename)

    @staticmethod
    def get_db_connection():
        connection = pymysql.connect(host='35.224.39.252',
                                    user='root',
                                    password='12345',
                                    db='homework5',
                                    charset='utf8mb4',
                                    cursorclass=pymysql.cursors.DictCursor)
        return connection


    def do_GET(self):
        country = self.headers['X-country']
        ip = self.headers['X-client-IP']
        gender = self.headers.get('X-gender', None)
        age = self.headers.get('X-age', None)
        print(f"Inserting age: {age}")
        income = self.headers.get('X-income', None)
        #time_of_day = datetime.datetime.now().time()
        time_of_day = datetime.datetime.now()
        #time_of_day = now.strftime('%H:%M:%S')
        parts = self.path.split('/')
        
        # Determine if banned:
        is_banned = 1 if country in ['North Korea', 'Iran', 'Cuba', 'Myanmar', 'Iraq', 'Libya', 'Sudan', 'Zimbabwe', 'Syria'] else 0        
        if len(parts) < 2:
            self.send_response(400)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(bytes("Bad request.", "utf-8"))

            # Insert into error log table
            with MyServer.get_db_connection() as connection:
                with connection.cursor() as cursor:
                    sql = """INSERT INTO error_logs (time_of_request, requested_file, error_code) VALUES (%s, %s, %s)"""
                    cursor.execute(sql, (datetime.datetime.now(), self.path, 400))
                connection.commit()
            return
        else:
            bucket = parts[0]
            filename = parts[1]
            self.send_gcs_response(bucket, filename)
            
            # Insert into main request table
            with MyServer.get_db_connection() as connection:
                with connection.cursor() as cursor:
                    sql = """INSERT INTO request_table (country, client_ip, gender, age, income, is_banned, time_of_day, requested_file) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
                    cursor.execute(sql, (country, ip, gender, age, income, is_banned, time_of_day, filename))
                connection.commit()


    def send_gcs_response(self, bucket, filename):
        receive_headers = self.headers
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket)
        blobname = filename
        blob = bucket.blob(blobname)

        try:
            content = ''
            with blob.open("r") as f:
                content = f.read()

            # Only send 200 response after successfully reading content
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(bytes("<html><head><title>https://pythonbasics.org</title></head>", "utf-8"))
            self.wfile.write(bytes("<p>Request: %s</p>" % self.path, "utf-8"))
            self.wfile.write(bytes("<body>", "utf-8"))
            self.wfile.write(bytes("<p>This is an example web server.</p>", "utf-8"))
            for key in receive_headers:
                self.wfile.write(bytes("Got header ", "utf-8"))
                self.wfile.write(bytes('{}:{}\n'.format(key, receive_headers[key]), "utf-8"))
            self.wfile.write(bytes("</body></html>", "utf-8"))
            self.wfile.write(bytes(content, "utf-8"))
        except:
            self.send_response(404)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(bytes("<html><head><title>https://pythonbasics.org</title></head>", "utf-8"))
            self.wfile.write(bytes("<p>Request: %s</p>" % self.path, "utf-8"))
            self.wfile.write(bytes("<body>", "utf-8"))
            self.wfile.write(bytes("<p>File not found.</p>", "utf-8"))
            self.wfile.write(bytes("</body></html>", "utf-8"))

            with MyServer.get_db_connection() as connection:
                with connection.cursor() as cursor:
                    sql = """INSERT INTO error_logs (time_of_request, requested_file, error_code) VALUES (%s, %s, %s)"""
                    cursor.execute(sql, (datetime.datetime.now(), self.path, 404))
                connection.commit()                
            
    def do_PUT(self):
        self.send500error()

    def do_POST(self):
        self.send500error()

    def do_HEAD(self):
        self.send500error()

    def do_DELETE(self):
        self.send500error()

    def send500error(self):
        self.send_response(500)
        self.end_headers()
        self.wfile.write(bytes("Server method unavailable", "utf-8"))
        with MyServer.get_db_connection() as connection:
            with connection.cursor() as cursor:
                sql = """INSERT INTO error_logs (time_of_request, requested_file, error_code) VALUES (%s, %s, %s)"""
                cursor.execute(sql, (datetime.datetime.now(), self.path, 501))
            connection.commit()
                    
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--domain", help="Domain to make requests to", type=str, default="localhost")
    parser.add_argument("-p", "--port", help="Server Port", type=int, default=8080)
    args = parser.parse_args()
    
    webServer = HTTPServer((args.domain, args.port), MyServer)
    print("Server started http://%s:%s" % (args.domain, args.port))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")
        
if __name__ == "__main__":        
    main()


