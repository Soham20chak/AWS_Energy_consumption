import json
import base64
import os
import boto3

s3_client = boto3.resource('s3')
s3_bucket_name = "energy-alert-data"

def lambda_handler(event, context):
    if 'Records' in event:
        for record in event['Records'] :
            sample_string_bytes = base64.b64decode(record['kinesis']['data'])
            sample_string = sample_string_bytes.decode("ascii")
            print(type(sample_string))
            s3_file_name =  record['kinesis']['sequenceNumber']+".json"
            print("s3 bucket file name  {}".format(s3_file_name))
            local_file_path = "/tmp/"+s3_file_name
            print("file path:{}".format(local_file_path))
            with open(local_file_path, 'w') as fp:
                json.dump(json.loads(sample_string), fp)
            print("event is stored in local json file")
            s3_client.meta.client.upload_file(local_file_path, s3_bucket_name, s3_file_name)
            print("file uploaded successfully..")
            os.remove(local_file_path)
            print("File deleted after upload to s3 bucket")
    else:
        return "No records found in the event."
