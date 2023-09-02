"""
Module for handling the downloading, loading and
processing of data from football-data.co.uk.

Author: Padraig Cleary
"""

from datetime import datetime
import io
import json
import time
from functools import cached_property
import re
import sys
from urllib.parse import urljoin
import urllib.parse

import awswrangler as wr
import boto3
from bs4 import BeautifulSoup
import pandas as pd
import requests

from football_pipeline import common, s3_utils

RAW_FOOTBALL_DATA_CO_UK_BUCKET = 'football-data-co-uk-raw'

CLEAN_FOOTBALL_DATA_CO_UK_COLUMNS = [
    'Div', 'Date', 'Time', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'FTR', 'HTHG', 'HTAG',
    'HTR', 'Referee', 'HS', 'AS', 'HST', 'AST', 'HF', 'AF', 'HC', 'AC',
    'HY', 'AY', 'HR', 'AR', 'B365H', 'B365D', 'B365A', 'BWH', 'BWD', 'BWA',
    'IWH', 'IWD', 'IWA', 'PSH', 'PSD', 'PSA', 'WHH', 'WHD', 'WHA', 'VCH',
    'VCD', 'VCA', 'MaxH', 'MaxD', 'MaxA', 'AvgH', 'AvgD', 'AvgA', 'B365>2.5', 'B365<2.5',
    'P>2.5', 'P<2.5', 'Max>2.5', 'Max<2.5', 'Avg>2.5', 'Avg<2.5', 'AHh', 'B365AHH', 'B365AHA', 'PAHH',
    'PAHA', 'MaxAHH', 'MaxAHA', 'AvgAHH', 'AvgAHA', 'B365CH', 'B365CD', 'B365CA', 'BWCH', 'BWCD',
    'BWCA', 'IWCH', 'IWCD', 'IWCA', 'PSCH', 'PSCD', 'PSCA', 'WHCH', 'WHCD', 'WHCA',
    'VCCH', 'VCCD', 'VCCA', 'MaxCH', 'MaxCD', 'MaxCA', 'AvgCH', 'AvgCD', 'AvgCA', 'B365C>2.5',
    'B365C<2.5', 'PC>2.5', 'PC<2.5', 'MaxC>2.5', 'MaxC<2.5', 'AvgC>2.5', 'AvgC<2.5', 'AHCh', 'B365CAHH', 'B365CAHA',
    'PCAHH', 'PCAHA', 'MaxCAHH', 'MaxCAHA', 'AvgCAHH', 'AvgCAHA'
]

COUNTRY_URL_LOOKUP = {
    'England': 'https://www.football-data.co.uk/englandm.php',
    'Scotland': 'https://www.football-data.co.uk/scotlandm.php',
    'Germany': 'https://www.football-data.co.uk/germanym.php',
    'Italy': 'https://www.football-data.co.uk/italym.php',
    'Spain': 'https://www.football-data.co.uk/spainm.php',
    'France': 'https://www.football-data.co.uk/francem.php',
    'Netherlands': 'https://www.football-data.co.uk/netherlandsm.php',
    'Belgium': 'https://www.football-data.co.uk/belgiumm.php',
    'Portugal': 'https://www.football-data.co.uk/portugalm.php',
    'Turkey': 'https://www.football-data.co.uk/turkeym.php',
    'Greece': 'https://www.football-data.co.uk/greecem.php',
}


def custom_date_parser(date_str: str) -> datetime.date:
    """
    Custom date parser to handle the inconsistent date formats,
    taking advantage of the regularity of the date format across
    all divisions, ie the day and month are always the first two
    parts of the date string, with either %y or %Y format used for the year.
    """

    day, month, year = date_str.split("/")
    
    if len(year) == 2:
        year_format = "%y"
    else:
        year_format = "%Y"
        
    date_format = f"%d/%m/{year_format}"

    return datetime.strptime(date_str, date_format).date()


def is_valid_col(s):
    is_not_empty = bool(s.strip()) and any(char.isalnum() for char in s)
    return is_not_empty


def clean_excess_delimiters(full_s3_path, encoding) -> pd.DataFrame:
    """
    Function to clean up any excess comma delimiter issues in the 
    raw football-data.co.uk csv files.

    Args:
        full_s3_path: the full s3 path to the file, e.g: s3://my-bucket/my-folder/my-file.csv
        encoding: the encoding to use when reading the file
    """

    # download the s3 object to a buffer object
    buffer = io.BytesIO()
    wr.s3.download(path=full_s3_path, local_file=buffer)

    with io.BytesIO() as outfile:
        i = 0
        for line in buffer.getvalue().decode(encoding).splitlines():

            if i == 0:
                column_list = [c for c in line.split(',') if is_valid_col(c)] # infer the expected number of columns from row 1
                expected_column_count = len(column_list)
                cleaned_header_line = ','.join(column_list)
                if '\n' not in cleaned_header_line:
                    cleaned_header_line = cleaned_header_line+'\n'
                outfile.write(cleaned_header_line.encode('utf-8'))
            else:
                columns = line.strip().split(',')
                if len(columns) > expected_column_count:  # could len(columns) ever be < expected_column_count?
                    columns = columns[:expected_column_count]
                cleaned_line = ','.join(columns) + '\n'
                outfile.write(cleaned_line.encode('utf-8'))
            i = i+1

        # If this fails; throw the error 
        outfile.seek(0)
        df = pd.read_csv(outfile)
    return df


def clean_raw_football_data_co_uk_csv_file(bucket_name, file_key) -> pd.DataFrame:
    """
    Function to remove null/empty rows, clean up the dates and delimiters in 
    the football-data.co.uk csv files.
    Some files can also be Windows-encoded, so need to consider that. 
    Returns:

    """
    s3_bucket_path = f"s3://{bucket_name}/{file_key}"
    try:
        df = wr.s3.read_csv(s3_bucket_path, encoding='utf-8')
    except UnicodeDecodeError:
        try:
            df = wr.s3.read_csv(s3_bucket_path, encoding='cp1252') # this is the only other encoding used that I've seen so far
        except pd.errors.ParserError:
            # indicative of the excess comma issue present in some files
            df = clean_excess_delimiters(s3_bucket_path, encoding='cp1252')
    except pd.errors.ParserError:
        try:
            df = clean_excess_delimiters(s3_bucket_path, encoding='utf-8')
        except UnicodeDecodeError:
            df = clean_excess_delimiters(s3_bucket_path, encoding='cp1252')

    # Drop null rows
    df.dropna(subset=['Div'], inplace=True)

    # Occasional files have inconsistent use of commas in Referee names, seems to have been a 2001/2002 season issue (all English divs)
    if 'Referee' in df.columns:
        df['Referee'] = df['Referee'].astype(str)  # bugfix for cases where Referee column is read in as object
        df['Referee'] = df.apply(lambda row: " ".join(row['Referee'].split(',')[::-1]) if ',' in row['Referee'] else row['Referee'], axis=1)
    
    # This is preferable to the previous method of hardcoding date formats based on season. Use custom parser to determine if it's %y or %Y
    df['Date'] = df['Date'].apply(custom_date_parser)

    df['HomeTeam'] = df['HomeTeam'].str.strip()
    df['AwayTeam'] = df['AwayTeam'].str.strip()

    # Add in any missing columns, otherwise the glue job will fail
    missing_columns = [col for col in CLEAN_FOOTBALL_DATA_CO_UK_COLUMNS if col not in df.columns]
    if missing_columns:
        df = pd.concat([df, pd.DataFrame(columns=missing_columns)], axis=1)

    unwanted_columns = [col for col in df.columns if col not in CLEAN_FOOTBALL_DATA_CO_UK_COLUMNS]
    if unwanted_columns:
        df.drop(columns=unwanted_columns, inplace=True)

    return df
    

def clean_footballdata_handler(event, context):
    """
    Lambda function handler that aims to apply some basic cleaning
    and team name mapping for the football-data.co.uk csv result files.
    Currently only implemented for EPL; other leagues are expected to have
    their own data quirks that will probably need further hand-crafted cleaning
    tools.

    Args:
        event: an S3 put event from football-data-co-uk-raw bucket
        context:

    Returns:

    """
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    # Decoding the file key
    file_key = urllib.parse.unquote_plus(file_key)
    cleaned_df = clean_raw_football_data_co_uk_csv_file(bucket_name, file_key)
    s3_utils.upload_df_to_s3('football-data-co-uk-clean', file_key=file_key, df=cleaned_df[CLEAN_FOOTBALL_DATA_CO_UK_COLUMNS])
    print(f"{file_key} cleaned and loaded to s3.")
    return {
        'statusCode': 200,
    }


class FootballDataCountry:
    """
    Class to handle the extract, load part of
    an ELT pipeline to process country-level results
    data from football-data.co.uk
    """

    def __init__(self, country_name: str):

        try:
            self._country_url = COUNTRY_URL_LOOKUP[country_name]
        except KeyError:
            print(f"country_name argument must be one of: {', '.join(COUNTRY_URL_LOOKUP.keys())}")
            raise

        self._soup = None
        self._all_season_urls = None
        self._all_seasons = None
        self._current_seasons = None
        self._country = country_name

    @property
    def country_url(self):
        return self._country_url

    @cached_property
    def soup(self):
        response = requests.get(self._country_url)
        self._soup = BeautifulSoup(response.content, 'html.parser')
        return self._soup

    @cached_property
    def all_season_urls(self):
        self._all_season_urls = self._get_season_level_urls()
        return self._all_season_urls

    @cached_property
    def all_seasons(self):
        self._all_seasons = [FootballDataSeason(url, self.country) for url in self.all_season_urls]
        return self._all_seasons

    @property
    def current_seasons(self):
        latest_year = max([s.year for s in self.all_seasons])
        self._current_seasons = [s for s in self.all_seasons if s.year == latest_year]
        return self._current_seasons

    @property
    def country(self):
        return self._country

    def __repr__(self):
        return f"FootballDataCountry(url='{self.country_url}')"

    def _get_season_level_urls(self):
        csv_links = [urljoin(self.country_url, a['href']) for a in self.soup.find_all('a', href=True) if '.csv' in a['href']]
        return csv_links

    def _get_country_string(self):
        return self.country_url.split('/')[-1].replace('m.php', '').capitalize()

    def load_current_seasons(self):
        """To be used for weekly/daily update jobs."""
        if self.current_seasons is not []:
            for season in self.current_seasons:
                season.upload_to_s3()
        return

    def load_all_seasons(self):
        """Useful for backfill jobs"""
        print(f"Starting load of {self.country}...")
        for season in self.all_seasons:
            season.upload_to_s3()
            time.sleep(1.5)  # precaution measure to avoid hammering the server


class FootballDataSeason:
    """
    Class to handle season-level result URLs
    from football_pipeline-data.co.uk
    """
    def __init__(self, season_url, country):
        self._season_url = season_url
        self._country = country
        self._season_code = None
        self._year = None
        self._league_code = None

    @property
    def season_url(self):
        return self._season_url

    @property
    def country(self):
        return self._country

    @property
    def season_code(self):
        self._season_code = self._extract_season_code_from_results_url()
        return self._season_code

    @property
    def year(self):
        return self._convert_season_code_to_year(self.season_code)

    @property
    def league_code(self):
        return self._extract_league_code_from_results_url()

    def __repr__(self):
        return f"FootballDataSeason(url='{self.season_url}', country='{self.country}')"

    def _extract_season_code_from_results_url(self) -> str:
        return self.season_url.split('/')[-2]

    def _extract_league_code_from_results_url(self) -> str:
        return self.season_url.split('/')[-1].replace('.csv','')

    def _convert_season_code_to_year(self, season_code) -> int:
        """
        Take a season code that follows the convention as exemplified by:
        1314 (== 2013-2014), 9798 (== 1997-1998), etc,
        return the full finish year for that given season.

        e.g:
        1516 -> 2016
        9798 -> 1998
        9900 -> 2000
        """
        season_code = str(season_code)
        first, second = season_code[:2], season_code[2:]
        if int(first) > 50 and int(first) < int(second):
            # means that we are looking at a 19__ year, since we are way before the YEAR 2050 and it's not the edge case for 1999-2000: 9900
            finish_year = 1900 + int(second)
        else:
            finish_year = 2000 + int(second)

        return finish_year

    def upload_to_s3(self):
        file_content = requests.get(self.season_url).content
        s3 = boto3.resource("s3")
        key_str = f"country={self.country}/league={self.league_code}/season={self.season_code}/{self.league_code}.csv"
        print(f"Start loading {key_str} to s3...")
        try:
            s3.Bucket(RAW_FOOTBALL_DATA_CO_UK_BUCKET).put_object(Key=key_str, Body=file_content)
            print("Load succeeded.")
        except Exception as e:
            print(e)


def scrape_results_handler(event, context):
    """
    Handler function AWS Lambda function to scrape the season
    results from football-data.co.uk for the current season or
    for all available years.

    Args:
        event:
        context:

    Returns:

    """
    mode = event.get('mode', 'update')  # Default to 'update' if mode isn't specified

    if mode == 'backfill':
        for country_url in COUNTRY_URL_LOOKUP:
            country = FootballDataCountry(country_url)
            country.load_all_seasons()
            time.sleep(0.5)
        return {
            'statusCode': 200,
            'body': json.dumps('Backfill mode executed successfully')
        }

    elif mode == 'update':
        for country_url in COUNTRY_URL_LOOKUP:
            country = FootballDataCountry(country_url)
            country.load_current_seasons()
        return {
            'statusCode': 200,
            'body': json.dumps('Update mode executed successfully')
        }
    else:
        return {
            'statusCode': 400,
            'body': json.dumps(f'Invalid mode: {mode}')
        }



if __name__ == "__main__":
    valid_functions = ["update", "backfill"]

    if len(sys.argv) < 2:
        print(f"Please provide a function name as an argument. Valid options are: {', '.join(valid_functions)}")
        sys.exit(1)
    func_name = sys.argv[1]
    event = {'mode': func_name}
    scrape_results_handler(event, {})
