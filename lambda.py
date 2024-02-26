import json
import base64
import boto3

def lambda_handler(event, context):
    sns_topic_arn = 'arn:aws:sns:us-east-1:896334692798:excess_alert'
    dynamodb=boto3.resource('dynamodb')
    table=dynamodb.Table('energy-alert-information')
    # Check if the 'Records' key exists in the event
    if 'Records' in event:
        for record in event['Records']:
            pk=record['kinesis']['partitionKey']
            # Decode the Kinesis record data (assuming it's in base64)
            kinesis_data = record['kinesis']['data']
            decoded_data = base64.b64decode(kinesis_data).decode('utf-8')
            
            # Process the data (you can customize this part)
            data = json.loads(decoded_data)
            energy_consumed = data.get('energy_consumed')
            message = f"High energy consumption detected at location {data['location']}. Energy consumed: {data['energy_consumed']}"
            # Only publish if energy consumption is greater than 12
            if energy_consumed > 12:
                sns_client = boto3.client('sns')
                sns_client.publish(TopicArn=sns_topic_arn, Message=message)
                table.put_item(Item={
                    "pk":pk,
                    "data":data
                })

        return "Processed {} records.".format(len(event['Records']))
    else:
        return "No records found in the event."
