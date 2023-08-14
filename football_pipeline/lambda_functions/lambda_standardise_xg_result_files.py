import boto3
import pandas as pd
from io import StringIO, BytesIO

s3 = boto3.client('s3')

expected_columns = [
    'round',
    'week',
     'day',
     'date',
     'time',
     'home',
     'score',
     'away',
     'attendance',
     'venue',
     'referee',
     'home_goals',
     'away_goals',
     'home_xg',
     'away_xg',
     'date_dt',
]


def handler(event, context):
    """
    Lambda function handler that checks for missing or inconsistent
    columns, missing data handling before writing to (league, season)
    partitioned s3 buckets in the Parquet format
    Args:
        event:
        context:

    Returns:

    """
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    csv_obj = s3.get_object(Bucket=bucket, Key=key)
    body = csv_obj['Body'].read().decode('utf-8')

    df = pd.read_csv(StringIO(body))

    # Check if the certain string exists in the file name
    if 'round' not in df.columns.tolist():
        df['round'] = 'N/A'
    if 'week' not in df.columns.tolist():
        df['week'] = -1.0

    # we should partition by season and league name , in the folder structure
    league_name = df['league_name'].values[0]
    season_code = '-'.join([d for d in key.split('-') if d.isdigit() and len(d) == 4])  # i only want 'year' digits to be caught here

    df.drop(columns=['league_name'], inplace=True)
    df['attendance'] = df['attendance'].astype(float)

    df['time'] = df['time'].fillna('missing')
    df['venue'] = df['venue'].fillna('missing')

    output_columns = expected_columns
    output_buffer = BytesIO()

    df[output_columns].to_parquet(output_buffer, index=False)
    output_buffer.seek(0)

    # adopt the '=' in the folders to help Glue infer the partition key column names:
    s3.put_object(Bucket='football_pipeline-xg-results-clean', Key=f"league={league_name}/season={season_code}/{key}.parquet",
                  Body=output_buffer.getvalue())
