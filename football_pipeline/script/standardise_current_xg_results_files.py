import json
import os

import boto3
import requests

s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')

bucket = 'football-data-co-uk-raw'
lambda_function_name = 'clean_football_data_co_uk'

paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket)

for page in pages:
    for obj in page['Contents']:
        key = obj['Key']

        # Prepare the input for the Lambda function
        lambda_input = {
            "Records": [
                {
                    "s3": {
                        "bucket": {
                            "name": bucket
                        },
                        "object": {
                            "key": key
                        }
                    }
                }
            ]
        }
        # I have set local env IS_LOCAL variable to true, so it should hit local running docker container
        #if os.getenv("IS_LOCAL"):
        #    response = requests.post("http://localhost:9000/2015-03-31/functions/function/invocations",
        #                             headers={"Content-Type": "application/json"},
        #                             data=json.dumps(lambda_input))
        #    response = response.json()
        #else:
        # Invoke the Lambda function
        response = lambda_client.invoke(
            FunctionName=lambda_function_name,
            InvocationType='Event',
            Payload=json.dumps(lambda_input)
        )
        print(f"{key} processed.")
