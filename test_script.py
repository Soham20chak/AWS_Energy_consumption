import boto3
import random
import time

aws_access_key_id='AKIA5BMNWAG7KLHIHLBR'
aws_secret_access_key='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'


# Initialize Kinesis client
kinesis_client = boto3.client('kinesis',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key, region_name='us-east-1')

# Name of the Kinesis Data Stream
stream_name = 'energy_data_stream'

# Function to generate simulated energy consumption data
location = random.choice(['home', 'office', 'factory'])
energy_consumption = round(random.uniform(1, 10), 2)  # Simulated energy consumption in kWh
timestamp = int(time.time())
temperature = round(random.uniform(15, 35), 2)  # Simulated temperature in Celsius
humidity = round(random.uniform(30, 70), 2)  # Simulated humidity in percentage
mydata = '{ "location": ' + str(location) + ', "energy_consumption": ' + str(energy_consumption) + ', "timestamp": ' + str(timestamp) + ', "temperature": ' + str(temperature) + ', "humidity": ' + str(humidity) + '}'
partition_key = str(location)
kinesis_client.put_record(
    StreamName=stream_name,
    Data=mydata,
    PartitionKey=partition_key
)
print("Sent data: ", mydata)
time.sleep(5)  # Adjust the frequency of data generation as needed
