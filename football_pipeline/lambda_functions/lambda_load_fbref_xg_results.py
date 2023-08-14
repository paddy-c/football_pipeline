"""
deprecated for now; not sure if we need
this xg results loader in lambda_consolidate_footballdata form yet,
maybe for weekly updating results in season?
"""
import os
import json
from expected_goals import fb_ref


def handler(event, context):
    version = os.environ['APP_VERSION']
    fb_ref.scrape_xg_result_seasons()

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"},
        "body": json.dumps({
            "Version ": version})}
