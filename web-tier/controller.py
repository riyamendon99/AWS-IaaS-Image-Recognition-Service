import time
import boto3
from botocore.exceptions import ClientError

#Set the AWS Region
AWS_REGION = 'us-east-1'
#Set the SQS Request Queue Name
AWS_SQS_REQUEST_QUEUE_NAME = f'{ASU_ID}-req-queue'
#Set the SQS REsponse Queue Name
AWS_SQS_RESPONSE_QUEUE_NAME = f'{ASU_ID}-resp-queue'

#Set the Maximum Instanes to 15
MAX_INSTANCES = 15
#Set the Delay to 5 seconds
SHUTDOWN_LIMIT = 5

#Instantiate the EC2 instance
ec2 = boto3.resource(service_name ='ec2', region_name=AWS_REGION)

#Instantiate the SQS Queue
sqs = boto3.client(service_name ='sqs', region_name=AWS_REGION)

#Fetch the Request Queue Url
response = sqs.get_queue_url(QueueName=AWS_SQS_REQUEST_QUEUE_NAME)
request_queue_url = response["QueueUrl"]

#Fetch the Response Queue Url
response = sqs.get_queue_url(QueueName=AWS_SQS_RESPONSE_QUEUE_NAME)
response_queue_url = response["QueueUrl"]


#A Function to launch the specific instances using instance ids
def start_instances(instance_ids):
    #Check if instance ids are null or not
    if instance_ids:
        print(f"Starting instances: {instance_ids}")
        try:
            #Filter the provided instances and launch the Instances
            ec2.instances.filter(InstanceIds=instance_ids).start()
        except ClientError as e:
            #Capture any error due to launching instances
            print(f"Error starting instances: {e}")

#A Function to stop the specific instance ids
def stop_instances(instance_ids):
    #Check if instance ids are null or not
    if instance_ids:
        print(f"Stopping instances: {instance_ids}")
        try:
            #Filter the provided instances and stop the Instances
            ec2.instances.filter(InstanceIds=instance_ids).stop()
        except ClientError as e:
            #Capture any error due to stopping instances
            print(f"Error stopping instances: {e}")


#A Function to fetch the instance id based on the instance state
def get_instances_by_state(state):
    #Filter based on the state parameter passed
    instances = ec2.instances.filter(
        Filters=[
            {'Name': 'tag:Name', 'Values': ['app-tier-instance-*']},
            {'Name': 'instance-state-name', 'Values': [state]}
        ]
    )
    #Return the instance ids of the filtered instances
    return [instance.id for instance in instances]


#A Function to calculate the length of the SQS Request Queue
def get_request_queue_length():
    try:
        #Fetch the Queue Attributes
        attributes = sqs.get_queue_attributes(
            QueueUrl=request_queue_url,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        #Return the size of the messages in the Request SQS Queue
        return int(attributes['Attributes']['ApproximateNumberOfMessages'])
    except ClientError as e:
        #Log any errors occured during the calculation of queue length
        print(f"Error fetching request queue length: {e}")
        return 0


#Main autoscale function
def autoscale():
    #Keep the Autoscale code running in a loop for processing
    while True:
        #Fetch the length of the Request SQS Queue
        length_of_request_sqs_queue = get_request_queue_length()

        #Filter the instances that are in the running state
        filter_running_instances = get_instances_by_state('running')

        #Filter the instances that are in the stopped state
        filter_stopped_instances = get_instances_by_state('stopped')

        #Calculate the number of instances that are in running state
        number_of_running_instances = len(filter_running_instances)

        #Check if the number of messages in the request queue is greater than the number of instances in running state and if it is less that the max limit of instances.
        if length_of_request_sqs_queue > number_of_running_instances and number_of_running_instances < MAX_INSTANCES:
            #Find the number of instances to launch by calculating minimum of the net value of number of messages in the sqs queue and number of running instances and the len of stopped instances
            instances_to_start = min(length_of_request_sqs_queue - number_of_running_instances, len(filter_stopped_instances))
            
            #Launch the calculated number of instances
            start_instances(filter_stopped_instances[:instances_to_start])

        #Check if numbe of messages in the sqs queue is zero and instances are still running
        elif length_of_request_sqs_queue == 0 and number_of_running_instances > 0:
            print("No pending requests. Stopping all instances within 5 seconds.")

            #Wait for 5 seconds before stopping the instances.
            time.sleep(SHUTDOWN_LIMIT)

            #Stop the instances.
            stop_instances(filter_running_instances)

        time.sleep(1)


if __name__ == "__main__":
    #Call the main autoscaling function
    autoscale()
