"""
Module for handling the downloading, loading and
processing of data from football-data.co.uk.

Author: Padraig Cleary
"""

import json
import time
from functools import cached_property
import sys
from urllib.parse import urljoin

import boto3
from bs4 import BeautifulSoup
import pandas as pd
import requests

from football_pipeline import common, s3_utils

RAW_FOOTBALL_DATA_CO_UK_BUCKET = 'football-data-co-uk-raw'

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

season_date_format = {
    '0607': '%d/%m/%y',
    '0708': '%d/%m/%y',
    '0809': '%d/%m/%y',
    '0910': '%d/%m/%y',
    '1011': '%d/%m/%y',
    '1112': '%d/%m/%y',
    '1213': '%d/%m/%y',
    '1314': '%d/%m/%y',
    '1415': '%d/%m/%y',

    '1516': '%d/%m/%Y',

    '1617': '%d/%m/%y',

    '1718': '%d/%m/%Y',
    '1819': '%d/%m/%Y',
    '1920': '%d/%m/%Y',
    '2021': '%d/%m/%Y',
    '2122': '%d/%m/%Y',
    '2223': '%d/%m/%Y',
}


def _get_date_format(season_string):
    """
    For EPL ('E0') data, look up the date format
    given the season_string, e.g '1718'
    Args:
        season_string:

    Returns:
        date_format: the date format to be used by pandas.to_datetime

    """
    for season, date_format in season_date_format.items():
        if season in season_string:
            return date_format


def concat_and_clean_footballdata_results() -> pd.DataFrame:
    """
    Concatenates all the EPL csv files into Pandas df;
    clean the date column and apply the fbref->footballdata.co.uk team name mapping
    for ease of future joining.
    Returns:

    """
    football_data_to_fbref_names = {v: k for k, v in common.FB_REF_FOOTBALL_DATA_TEAM_MAP.items()}
    df = s3_utils.consolidate_all_bucket_csvs('football_pipeline-lake', 'E0')
    df.dropna(subset=['Div'], inplace=True)
    # clean the dates, since different seasons have particular formats(!)
    df['date_format'] = df['season'].apply(lambda row: _get_date_format(row))
    df['date_new'] = df.apply(lambda row: pd.to_datetime(row['Date'], format=row['date_format']), axis=1)
    # add the fbref team name equivalents...for easy joining later
    df['home_fbref'] = df['HomeTeam'].map(football_data_to_fbref_names)
    df['away_fbref'] = df['AwayTeam'].map(football_data_to_fbref_names)

    return df


def consolidate_footballdata_handler(event, context):
    """
    Lambda function handler that aims to apply some basic cleaning
    and team name mapping for the football-data.co.uk csv result files.
    Currently only implemented for EPL; other leagues are expected to have
    their own data quirks that will probably need further hand-crafted cleaning
    tools.

    Args:
        event:
        context:

    Returns:

    """
    consolidated_df = concat_and_clean_footballdata_results()
    s3_utils.upload_df_to_s3('football_pipeline-processed', 'football_data_co_uk/epl_results_consolidated.csv', consolidated_df)


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
            time.sleep(0.5) # precaution measure to avoid hammering the server


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
            time.sleep(0.5)

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
