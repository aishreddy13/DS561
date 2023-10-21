#import functions_framework
from flask import Request, Response, Flask,request
from google.cloud import storage
from google.cloud import pubsub_v1
import logging

app = Flask(__name__)

banned = ['north korea', 'iran', 'cuba', 'myanmar', 'iraq', 'libya', 'sudan', 'zimbabwe', 'syria']

# def get_file_name(request: Request):
#     url_path = request.path
#     file_name = url_path.split('/')[-1]
#     return file_name


@app.route('/<path:file_path>',methods=['GET','PUT','POST','DELETE','HEAD','CONNECT','OPTIONS','TRACE','PATCH'])
#@functions_framework.http
def serve_file(file_path):
   # logging_client = logging.Client()
   # logging_client.setup_logging(log_level=logging.ERROR)
    logging.basicConfig(level=logging.INFO)
    file_path = file_path.rsplit('/',1)[1]
    method = request.method
    if method != 'GET':
        #logging.warning(f"Method not implemented: {method}, Path: {request.path}")  # Log the error
        logging.warning(f"Method not implemented: {method}, Path: {request.path}")
        print("Method not implemented: ", method)
        return 'Not Implemented', 501

    else:
        country = None
        # getting country 
        if 'X-country' in request.headers:
            country = request.headers.get("X-country").lower().strip()
            print("Country: ", country)
        else:
            print("No Country")

        if country not in banned:
            print("Permission Granted")
            logging.info("Permission Granted")
            #file_name = get_file_name(request)
            client = storage.Client()
            bucket = client.bucket('u62138442_hw2_b2')            
            blob = bucket.blob(file_path)
            try:
                content = blob.download_as_text()
                response = Response(content, status=200, headers={'Content-Type': 'text/html'})
                return response

            except Exception as e:
                #logging.error(f"File not found: {file_name}, Error: {str(e)}")
                logging.error(f"File not found: {file_path}, Error: {str(e)}")
                print("File not found: ",file_path)
                return str(e), 404
            
        else:
            print("Permission Denied")
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path('u62138442-hw2', 'topic_1')
    
            # bytestring data
            data_str = f"{country}"
            data = data_str.encode("utf-8")
            
            # try to publish
            try:
                future = publisher.publish(topic_path, data)
                future.result()  # Wait for the publish operation to complete
                print("Published to Pub/Sub successfully")
            except Exception as e:
                print("Error publishing to Pub/Sub:", str(e))
                return "Publish Denied", 400
            return "Permission Denied", 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
