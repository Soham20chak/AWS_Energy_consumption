import boto3
import random
import time

aws_access_key_id='AKIA5BMNWAG7KLHIHLBR'
aws_secret_access_key='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'


# Initialize Kinesis client
kinesis_client = boto3.client('kinesis',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key, region_name='us-east-1')

# Name of the Kinesis Data Stream
stream_name = 'energy_data_stream'

for x in range(1, 50):
    loc = random.randint(1, 3)
    energy = random.randint(1, 15)
    temp = random.randint(5,40)
    humidity = random.randint(30,70)
    mydata = '{ "location": ' + str(loc) + ', "energy_consumed": ' + str(energy) + ', "temperature": ' + str(temp) + ', "humidity": ' + str(humidity) + '}'
    partitionkey = random.randint(10, 100);
    response = kinesis_client.put_record(StreamName='energy_data_stream', Data=mydata, PartitionKey=str(partitionkey))
    print("Ingestion Done")
