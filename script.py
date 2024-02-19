import boto3
import json
import random
import time

# Initialize Kinesis client
kinesis_client = boto3.client('kinesis', region_name='your-region')

# Name of the Kinesis Data Stream
stream_name = 'your-stream-name'

# Function to generate simulated energy consumption data
def generate_energy_data():
    device_id = random.randint(1, 100)
    device_type = random.choice(['smart_meter', 'analog_meter'])
    location = random.choice(['home', 'office', 'factory'])
    energy_consumption = round(random.uniform(1, 10), 2)  # Simulated energy consumption in kWh
    timestamp = int(time.time())
    temperature = round(random.uniform(15, 35), 2)  # Simulated temperature in Celsius
    humidity = round(random.uniform(30, 70), 2)  # Simulated humidity in percentage
    return {
        'device_id': device_id,
        'device_type': device_type,
        'location': location,
        'energy_consumption': energy_consumption,
        'timestamp': timestamp,
        'temperature': temperature,
        'humidity': humidity
    }

# Main function to send data to Kinesis
def send_data_to_kinesis():
    while True:
        data = generate_energy_data()
        partition_key = str(data['device_id'])
        payload = json.dumps(data)
        kinesis_client.put_record(
            StreamName=stream_name,
            Data=payload,
            PartitionKey=partition_key
        )
        print("Sent data: ", data)
        time.sleep(1)  # Adjust the frequency of data generation as needed

if __name__ == '__main__':
    send_data_to_kinesis()
