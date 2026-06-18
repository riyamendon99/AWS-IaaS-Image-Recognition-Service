import time
import uuid
import base64
import threading
import boto3
from flask import Flask, request, jsonify, Response
import concurrent.futures
from botocore.exceptions import ClientError
from botocore.exceptions import NoCredentialsError
import os
from PIL import Image
import io

#Set the AWS Region
AWS_REGION = 'us-east-1'
#Set the SQS Request Queue Name
AWS_SQS_REQUEST_QUEUE_NAME = f'{ASU_ID}-req-queue'
#Set the SQS Response Queue Name
AWS_SQS_RESPONSE_QUEUE_NAME = f'{ASU_ID}-resp-queue'
#Set the S3 Input Bucket Name
AWS_S3_BUCKET_NAME = f'{ASU_ID}-in-bucket'

#Instantiate the SQS Queue
sqs = boto3.client(
    service_name = 'sqs',
    region_name=AWS_REGION
)

#Instantiate the S3 Bucket
s3_client = boto3.client(
        service_name ='s3',
        region_name =AWS_REGION
    )

#Fetch the Request Queue Url
response = sqs.get_queue_url(QueueName=AWS_SQS_REQUEST_QUEUE_NAME)
request_queue_url = response["QueueUrl"]

#Fetch the Response Queue Url
response = sqs.get_queue_url(QueueName=AWS_SQS_RESPONSE_QUEUE_NAME)
response_queue_url = response["QueueUrl"]

#Instantiate the thread pool for concurrent processing
executor = concurrent.futures.ThreadPoolExecutor(max_workers=100)

#Set the cached data initially to null
cache_data = {}

#Set the thread lock
locked_cache = threading.Lock()

#A Function to cache the responses so that the responses can be fetched quickly
def cache_func(operation, message_id=None, content=None):
    #Check if cache is locked or not
    with locked_cache:
        #Check if the operation is write then save the result in the cache
        if operation == "write" and message_id and content is not None:
            #Save the message id and the result to the cache
            cache_data[message_id] = content
        #Check if the operation is cleanup then, clear the cache
        elif operation == "cleanup":
            #This line clears the cached results
            cache_data.clear()
        #Check if the operation is read then return the result
        elif operation == "read":
            #Return the prediction result
            return cache_data.get(message_id, None)

#A Function to pull the messages from the response queue
def pull_from_response_sqs(message_id):
    #Keep the function running the process the response queue
    while True:
        #If the response is already cached, return the result
        response = cache_func("read", message_id=message_id)
        if response:
            #Check if the result is cached if yes then return the result
            return response
        
        #Add time delay to fetch the message from SQS Response Queue
        time.sleep(5)

        #Fetch the response from the Response SQS Queue along with All Attributes
        messages_response = sqs.receive_message(
            QueueUrl=response_queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5, 
            MessageAttributeNames=["All"], 
        )
        
        #Check if the response is null or not
        if "Messages" in messages_response:
            #Loop through each message received in the response
            for message in messages_response["Messages"]:
                #Get the Message attributes from each message response
                attributes = message.get("MessageAttributes", {})

                #Get the file name from the response queue
                file_name_from_response_queue = attributes.get("filename", {}).get("StringValue")

                #Get the message id passed in the response queue
                message_id_from_response_queue = attributes.get("message_id", {}).get("StringValue")

                #Get the prediction result in the message body
                result = message["Body"]
                
                #Store the response in the cached data so that later we can fetch the results quickly, reducing latency
                cache_func("write",message_id=message_id_from_response_queue, content={"filename": file_name_from_response_queue, "result": result})
                
                #If the message id from the request queue is same as the message id in the resposne, return the result
                if message_id_from_response_queue == message_id:
                    #Add time delay before deleting the message
                    time.sleep(5)
                    sqs.delete_message(
                        QueueUrl=response_queue_url,
                        ReceiptHandle=message["ReceiptHandle"],
                    )

                    #Return the file name and the prediction result
                    return {"filename": file_name_from_response_queue, "result": result}
                sqs.delete_message(
                    QueueUrl=response_queue_url,
                    ReceiptHandle=message["ReceiptHandle"],
                )


#A Function to push the message in the request sqs queue
def push_to_request_sqs(file_name):

    #Randomly generated message id for fetching the result from response queue
    message_id = str(uuid.uuid4())
    
    #Set the message attributes with message id and filename to send to the request queue
    attributes = {
        "message_id": {"StringValue": message_id, "DataType": "String"},
        "filename": {"StringValue": file_name, "DataType": "String"},
    }
    
    #Send the message with message id as body and message attributes to the request sqs queue
    response = sqs.send_message(
        QueueUrl=request_queue_url,
        MessageBody=message_id,
        MessageAttributes=attributes
    )

    #Return the message id so that we can compare it with the response message id  to identify the correct prediction result for the requested file
    return message_id 


#A Function to upload the encoded image to the S3 Bucket
def upload_to_s3(file_obj, filename):
    try:
        #s3_client.upload_fileobj(file_obj, AWS_S3_BUCKET_NAME, filename)

        #Set the  as filename and the value as the encoded image in S3 bucket
        s3_client.put_object(Bucket=AWS_S3_BUCKET_NAME, filename, Body=file_obj)
        return True
    except NoCredentialsError:
        #Check for any exceptions
        return False


#Instantiate the web tier flask application
app = Flask(__name__)
@app.route("/", methods=["POST"])
def main():
    #Fetch the imput file from the user to process the request
    file = request.files["inputFile"]

    #Read the requested file data
    fileData = file.read()

    #Encode the image
    encoded_image = base64.b64encode(fileData).decode("utf-8")

    #Split the file into filename and extention
    filename_without_extension, extension = os.path.splitext(file.filename)

    #Concurrently run the upload to S3 function to upload the images to S3
    future = executor.submit(upload_to_s3, encoded_image, filename_without_extension)
    
    #Capture the status of the S3 upload
    upload_success = future.result()
    if not upload_success:
        #Check if there are no errors while uploading file data to S3 Bucket
        return Response(f"Failed to Upload file to S3", status=500)
    

    #Send the request to the request SQS queue
    #Fetch the messgae id generated by the request queue
    message_id = push_to_request_sqs(file.filename)

    #Concurrently fetch the messages from the response queue
    future = executor.submit(pull_from_response_sqs, message_id)

    #Check for the status of the fetch from response sqs queue
    response = future.result()
    
    if response:
        #Return the prediction result back to the user
        return f"{response['filename']}:{response['result']}", 200


if __name__ == "__main__":
    #Set the port and launch the web tier
    app.run(host="0.0.0.0", port=8000, threaded=True)
