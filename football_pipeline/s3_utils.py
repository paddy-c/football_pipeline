from io import StringIO
import logging
from typing import Set

import boto3
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import ClientError
import pandas as pd

s3 = boto3.client("s3")


def upload_df_to_s3(bucket_name: str, file_key: str, df: pd.DataFrame):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3 = boto3.client("s3")
    response = s3.put_object(
        Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=file_key
    )
    print(response)

    return response


def get_scraped_urls(
    bucket_name="football-misc", file_key="scraped_links.txt"
) -> Set:
    """ """
    s3 = boto3.client("s3")

    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    body = obj["Body"].read()

    file_content = body.decode("utf-8")

    return set(file_content.splitlines())


def create_bucket(bucket_name, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        if region is None:
            s3_client = boto3.client("s3")
            s3_client.create_bucket(Bucket=bucket_name)
        elif region != "us-east-1":
            s3_client = boto3.client("s3", region_name=region)
            location = {"LocationConstraint": region}
            s3_client.create_bucket(
                Bucket=bucket_name, CreateBucketConfiguration=location
            )
        else:
            s3_client = boto3.client("s3", region_name=region)
            s3_client.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket"""
    if object_name is None:
        object_name = file_name

    try:
        s3.upload_file(file_name, bucket, object_name)
        print(f"{file_name} uploaded to {bucket} as {object_name}.")
    except NoCredentialsError as e:
        print(f"No credentials provided. {e}")


def download_file(file_name, bucket, object_name=None):
    """Download a file from an S3 bucket"""
    if object_name is None:
        object_name = file_name

    try:
        s3.download_file(bucket, object_name, file_name)
        print(f"{object_name} downloaded from {bucket} as {file_name}.")
    except NoCredentialsError as e:
        print(f"No credentials provided. {e}")


def list_objects(bucket):
    """List all objects in an S3 bucket"""
    try:
        response = s3.list_objects(Bucket=bucket)
        for obj in response["Contents"]:
            print(obj["Key"])
    except NoCredentialsError as e:
        print(f"No credentials provided. {e}")


def delete_object(bucket, object_name):
    """Delete an object from an S3 bucket"""
    try:
        s3.delete_object(Bucket=bucket, Key=object_name)
        print(f"{object_name} has been deleted from {bucket}.")
    except NoCredentialsError as e:
        print(f"No credentials provided. {e}")


def update_scraped_url_list(bucket_name, file_key, new_urls):
    # First, read the existing file.
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    body = obj["Body"].read()
    existing_urls = body.decode("utf-8").splitlines()

    # Add the new URLs.
    updated_urls = existing_urls + new_urls

    # Convert the list of URLs back to a string.
    updated_content = "\n".join(updated_urls)

    # Write the updated content back to S3.
    s3.put_object(Body=updated_content, Bucket=bucket_name, Key=file_key)


def update_team_lineups_file(bucket_name, file_key, new_urls):
    """

    Args:
        bucket_name:
        file_key:
        new_urls:

    Returns:

    """
    # First, read the existing file.
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    body = obj["Body"].read()
    existing_urls = body.decode("utf-8").splitlines()

    # Add the new URLs.
    updated_urls = existing_urls + new_urls

    # Convert the list of URLs back to a string.
    updated_content = "\n".join(updated_urls)

    # Write the updated content back to S3.
    s3.put_object(Body=updated_content, Bucket=bucket_name, Key=file_key)


def consolidate_all_bucket_csvs(
    bucket: str = "football_pipeline-lineups-and-managers",
    s3_prefix: str = "Premier-League/",
) -> pd.DataFrame:
    """
    Take an s3 bucket, having .csv objects of all the same schema,
    return a consolidated pandas dataframe from all the csv files.
    Args:
        bucket:
        s3_prefix:

    Returns:

    """
    data = pd.DataFrame()

    kwargs = {"Bucket": bucket, "Prefix": s3_prefix}
    while True:
        response = s3.list_objects_v2(**kwargs)
        for obj in response["Contents"]:
            if obj["Key"].endswith(".csv"):
                csv_obj = s3.get_object(Bucket=kwargs["Bucket"], Key=obj["Key"])
                body = csv_obj["Body"].read().decode("utf-8")
                df = pd.read_csv(StringIO(body))
                df["season"] = str(obj["Key"])
                data = pd.concat([data, df])
        try:
            kwargs["ContinuationToken"] = response["NextContinuationToken"]
        except KeyError:
            break

    return data
