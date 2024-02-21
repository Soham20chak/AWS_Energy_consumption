import json
import boto3

def lambda_handler(event, context):
    # Initialize SNS client
    sns_client = boto3.client('sns')
    
    # Iterate over records in the Kinesis event
    for record in event['Records']:
        # Decode and parse the data from the Kinesis record
        payload = json.loads(record['kinesis']['data'])
        
        # Check if the energy consumed is more than 12
        if payload['energy_consumed'] > 12:
            # Compose message for SNS notification
            message = f"High energy consumption detected at location {payload['location']}. Energy consumed: {payload['energy_consumed']}"
            
            # Publish message to SNS topic
            sns_client.publish(
                TopicArn='YOUR_SNS_TOPIC_ARN',
                Message=message,
                Subject='High Energy Consumption Alert'
            )
            
    return {
        'statusCode': 200,
        'body': json.dumps('SNS notification sent successfully')
    }
