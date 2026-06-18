from torch.utils.data import DataLoader
import torch
import boto3
from facenet_pytorch import MTCNN, InceptionResnetV1
import base64
from PIL import Image
from torchvision import datasets
import os
import subprocess

#Here Initializing the face detector with the specified parameters
mtcnn = MTCNN(image_size=240, margin=0, min_face_size=20)

#Loading the pretrained model using weights for face recognition
resnet = InceptionResnetV1(pretrained="vggface2").eval()

#Set the AWS Region
AWS_REGION = 'us-east-1'
#Set the SQS Request Queue Name
AWS_SQS_REQUEST_QUEUE_NAME = f'{ASU_ID}-req-queue'
#Set the SQS Response Queue Name
AWS_SQS_RESPONSE_QUEUE_NAME = f'{ASU_ID}-resp-queue'

#Set the S3 Input Bucket Name
AWS_S3_INPUT_BUCKET_NAME = f'{ASU_ID}-in-bucket'

#Set the S3 Output Bucket Name
AWS_S3_OUTPUT_BUCKET_NAME = f'{ASU_ID}-out-bucket'

#Instantiate the S3 Bucket
s3 = boto3.client(
    service_name = "s3",
    region_name=AWS_REGION
)

#Instantiate the SQS Queue
sqs = boto3.client(
    service_name = "sqs",
    region_name=AWS_REGION,
)

#Fetch the Request Queue Url
response = sqs.get_queue_url(QueueName=AWS_SQS_REQUEST_QUEUE_NAME)
request_queue_url = response["QueueUrl"]

#Fetch the Response Queue Url
response = sqs.get_queue_url(QueueName=AWS_SQS_RESPONSE_QUEUE_NAME)
response_queue_url = response["QueueUrl"]


#A Function that sets the local path to save the image temporarily
def get_local_path(file_name):
    local_path = f"/tmp/{file_name}"
    #Return the local path to save the image here
    return local_path


#A Function to push the prediction result in the Output S3 bucket
def push_to_output_s3( value):
    try:
        #Push the Prediction result in the Output S3 Bucket
        #Here  is the filename and Value is the Prediction Result
        s3.put_object(Bucket=AWS_S3_OUTPUT_BUCKET_NAME, Body=value)
        #print(f"{value} uploaded to S3 bucket '{AWS_S3_OUTPUT_BUCKET_NAME}' as '{}'.")
    except Exception as e:
        #Check for any exceptions when uploading the prediction result to the Output S3 bucket
        print(f"Error uploading data: {e}")

#This is the provided Face Recognition Function for this Project
def face_recognition(path_to_image):
    #Opening the input image
    img = Image.open(path_to_image)
    #Detects the face in the image
    face, prob = mtcnn(img, return_prob=True)
    #Send the detected face to the model to extract the face embedings.
    emb = resnet(face.unsqueeze(0)).detach()
    #Fetch the names from the data.pt file
    saved_data = torch.load("data.pt")
    #Retrieve the list of the embeddings
    embedding_list = saved_data[0]
    #Retrieve the list of Names
    name_list = saved_data[1] 

    #Initialize a list to save the distance between input embedings.
    dist_list = []

    #Loop through the embedings to calculate the distance from input embedings
    for idx, emb_db in enumerate(embedding_list):
        #Calculate the euclidean distance
        dist = torch.dist(emb, emb_db).item() 
        #Append the distance to the distance list
        dist_list.append(dist)
    
    #Get the minimum distance index from the distance list
    idx_min = dist_list.index(min(dist_list))
    
    #Return the predicted name
    return name_list[idx_min]

#Main Function to process the request
def main():
    #Keep the loop running to process thr requests
    while True:
        #Fetch the messages from the request queue for processing
        #Fetch All the message attributes from the request queue
        messages_response = sqs.receive_message(
            QueueUrl=request_queue_url,
            MaxNumberOfMessages=1,  
            WaitTimeSeconds=5,
            MessageAttributeNames=["All"]
        )
        
        #Check if the response is null or not
        if "Messages" in messages_response:
            #Loop through each of the messages from the request queue
            for message in messages_response["Messages"]:
                #Fetch the message id from the body
                message_id = message["Body"]

                #Fetch all the message attributes of the message
                attributes = message["MessageAttributes"]  

                #Fetch the message id from the  sqs request queue
                message_id = attributes["message_id"]["StringValue"]

                #Fetch the file name from the message attributes of the request queue 
                file_name = attributes["filename"]["StringValue"]

                #Split the filename into name and extension
                filename_without_extension = file_name.rstrip(".jpg")

                #Fetch the image from the S3 Bucket based on the filename received
                response = s3.get_object(Bucket=AWS_S3_INPUT_BUCKET_NAME, filename_without_extension)

                #Read the Encoded Image fetched from the S3 Bucket
                encoded_data = response['Body'].read().decode('utf-8')

                #Decode the Image from the S3 Bucket
                image_data = base64.b64decode(encoded_data)

                #Get the local path for temporary storage of the image
                image_path = get_local_path(filename_without_extension)
                
                #Write the Image to the local path for prediction
                with open(image_path, "wb") as img_file:
                    #Open and write the encoded data to the local path
                    img_file.write(image_data)
                
                #Send the image path to the procided face recognition algorithm for prediction
                result_of_face_recognition = face_recognition(image_path)

                #Push the prediction result to the Output S3 Bucket
                push_to_output_s3(file_name, result_of_face_recognition)

                #Curate the attribute list to send back to the response queue
                message_attributes = {
                    "message_id": {"StringValue": message_id, "DataType": "String"},
                    "filename": {"StringValue": filename_without_extension, "DataType": "String"},
                }

                #Send the message attributes containing the message id and filename along with the result of face recognition in the message body to the response sqs queue
                sqs.send_message(
                    QueueUrl=response_queue_url,
                    MessageBody=result_of_face_recognition,
                    MessageAttributes=message_attributes,
                )

                #Delete the message from SQS Queue after processing is complete
                sqs.delete_message(
                    QueueUrl=request_queue_url, 
                    ReceiptHandle=message["ReceiptHandle"]
                )

                #Remove the image from the temporary path after the processing is complete
                os.remove(image_path)


if __name__ == "__main__":
    #Call the main function of backend file
    main()
