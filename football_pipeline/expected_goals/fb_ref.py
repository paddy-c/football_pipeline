"""
Module for handling the extraction and
processing of data from FBref.com.

Author: Padraig Cleary
"""

import datetime as dt
from functools import cached_property
import io
import json
import os
import re
import sys
import time
import urllib.parse

import boto3
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from football_pipeline import s3_utils

FB_REF_COLUMNS = [
    "insertion_date",
    "league_name",
    "week",
    "day",
    "date",
    "time",
    "home",
    "home_xg",
    "score",
    "away_xg",
    "away",
    "attendance",
    "venue",
    "referee",
    "home_goals",
    "away_goals",
]

BASE_URL = "https://fbref.com"

QUEUE_URL = os.environ.get("SQS_QUEUE_URL")

COMPETITION_NUMBER_LEAGUE_MAP = {
    "9": "Premier-League",
    "10": "Championship",
    "12": "La-Liga",
    "22": "Major-League-Soccer",
    "13": "Ligue-1",
    "20": "Bundesliga",
    "11": "Serie-A",
    "15": "League-One",
}

# These URLs are the current season results pages for each league
SCORES_HOME_PAGE_URLS = {
    "Premier-League": "https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures",
    "Championship": "https://fbref.com/en/comps/10/schedule/Championship-Scores-and-Fixtures",
    "La-Liga": "https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures",
    "Major-League-Soccer": "https://fbref.com/en/comps/22/schedule/Major-League-Soccer-Scores-and-Fixtures",
    "Ligue-1": "https://fbref.com/en/comps/13/schedule/Ligue-1-Scores-and-Fixtures",
    "Bundesliga": "https://fbref.com/en/comps/20/schedule/Bundesliga-Scores-and-Fixtures",
    "Serie-A": "https://fbref.com/en/comps/11/schedule/Serie-A-Scores-and-Fixtures",
    "League-One": "https://fbref.com/en/comps/15/schedule/League-One-Scores-and-Fixtures",
}


class FBrefSeasonResultsPage:
    """
    Class to handle a fbref.com season results
    page url.
    """
    #TODO: apply access protection to the derived attributes

    def __init__(self, season_results_url: str):
        """
        Initialise FBrefSeasonResultsPage handler object.

        Args:
            season_results_url: fbref.com season results page,
            e.g: https://fbref.com/en/comps/9/2022-2023/schedule/2022-2023-Premier-League-Scores-and-Fixtures
        """
        self.link = season_results_url
        self.url_soup = self._get_soup_object()
        self.is_current_season = self._is_current_season()
        self.league_name = self._get_league_name_from_url()
        self._output_file_name = None
        self.processed_xg_df = self.preprocess_fbref_xg_results()
        self.match_level_urls = self.get_match_links_from_dom()

    @cached_property
    def output_file_name(self) -> str:
        """
        Generate the output file name for the processed
        season results file.

        Returns:
            output_file_name
        """

        if self.is_current_season:
            # season string of current seasons is not encoded in the URL, unlike previous seasons, workaround for now
            header = [h2 for h2 in self.url_soup.find_all('h2') if 'Fixtures' in h2.text][0].span.text
            try:
                season_string = re.search(r"(\d{4}-\d{4})", header).group(1)
            except AttributeError:  # catch MLS season
                season_string = re.search(r"(\d{4})", header).group(1)
            self._output_file_name = f"{season_string}-{self.link.split('/')[-1]}"
        else:
            self._output_file_name = self.link.split("/")[-1] 
        return self._output_file_name

    def _get_soup_object(self) -> bs:
        """
        Get the BeautifulSoup object for the given season
        level URL.

        Returns:
            url_soup
        """
        print(f"Scraping: {self.link}")
        response = requests.get(self.link)
        time.sleep(3.2)
        # can re-use this soup object for more efficient crawling, fewer requests
        url_soup = bs(response.text, "html.parser")
        return url_soup

    def _get_league_name_from_url(self) -> str:
        """
        Extract the competition number from the season URL and return the
        full league name from look up table.

        Returns:
            league_name
        """
        comp_no_match = re.search(r"https://fbref.com/en/comps/(\d+)/", self.link)
        comp_no = comp_no_match.groups(0)[0]
        league_name = COMPETITION_NUMBER_LEAGUE_MAP[comp_no]
        return league_name

    def get_match_links_from_dom(self):
        """Get the links to the detailed match stat pages for a given season soup object"""
        match_link_divs = self.url_soup.findAll(
            attrs={"data-stat": "score", "class": "center"}
        )
        links = [
            "https://fbref.com" + div.a["href"]
            for div in match_link_divs
            if div.a is not None
        ]
        return links

    def _is_current_season(self):
        """Utility func to check if current page is the current season."""
        links = self.url_soup.findAll("a", href=True, text="Next Season")
        current_season = True if not links else False
        return current_season

    def preprocess_fbref_xg_results(self) -> pd.DataFrame:
        """
        Take the 'soup' object of an fbref season xg results URL and apply
        the necessary cleaning and preprocessing before later storing as a
        file for further analysis and transformation.
        Args:
            url_soup:

        Returns:

        """

        table = self.url_soup.find_all("table")
        xg_results_df = pd.read_html(str(table))[0]

        # TODO: fix for now, but verify why there would be rows with no fields except home, away team...(fixtures?)
        xg_results_df = xg_results_df[~xg_results_df["Score"].isnull()]
        xg_results_df = xg_results_df[
            ~xg_results_df["Date"].str.contains("Date")
        ]
        # take out separator rows:
        separator_rows = xg_results_df[xg_results_df.Date.isnull()].index
        xg_results_df = xg_results_df[
            ~xg_results_df.index.isin(separator_rows)
        ]
        xg_results_df["league_name"] = self.league_name
        # Preprocessing
        xg_results_df["Score"] = xg_results_df["Score"].str.replace(
            r"\(.*\) ", ""
        )
        xg_results_df["Score"] = xg_results_df["Score"].str.replace(
            r" \(.*\)", ""
        )
        scoreline = xg_results_df["Score"].str.split("–", n=1, expand=True)
        # TODO: EFL matches can go to extra time in playoffs, and can have (3) 1 - 1 (4) scorelines for penalties...need to clean

        xg_results_df.loc[:, "home_goals"] = scoreline[0]
        xg_results_df.loc[:, "away_goals"] = scoreline[1]
        xg_results_df.rename(
            columns={"xG": "home_xg", "xG.1": "away_xg", "Wk": "week"}, inplace=True
        )
        if "home_xg" not in xg_results_df.columns.tolist():
            xg_results_df["home_xg"] = np.nan
            xg_results_df["away_xg"] = np.nan
        xg_results_df.drop(columns=["Match Report", "Notes"], inplace=True)
        xg_results_df.columns = [
            col.lower() for col in xg_results_df.columns.tolist()
        ]

        # Exclude any matches yet to be played:
        today_date = dt.datetime.today().strftime("%Y-%m-%d")
        xg_results_df["date_dt"] = pd.to_datetime(
            xg_results_df["date"], format="%Y-%m-%d"
        )
        xg_results_df = xg_results_df[
            xg_results_df["date_dt"] < today_date
            ]

        return xg_results_df

    def save_to_s3(self):
        """
        Save the processed season file to the football-xg-results
        S3 bucket and update the S3 scraped_links file with the new link.
        Returns:
        """
        s3_utils.upload_df_to_s3('football-xg-results', self.output_file_name+".csv", self.processed_xg_df)
        if not self.is_current_season:  # we always want to keep trying the current season link for freshly completed matches
            # so the current season NEVER gets inserted into the 'scraped season table' until new season starts.
            s3_utils.update_scraped_url_list('football-misc', 'scraped_links.txt', [self.link])


def _generate_scores_url(comp_no, year, league_name):
    if league_name == "Major-League-Soccer":
        full_url = f"https://fbref.com/en/comps/{comp_no}/{year}/schedule/{year}-Major-League-Soccer-Scores-and-Fixtures"
    else:
        full_url = f"https://fbref.com/en/comps/{comp_no}/{year-1}-{year}/schedule/{year-1}-{year}-{COMPETITION_NUMBER_LEAGUE_MAP[str(comp_no)]}-Scores-and-Fixtures"
    return full_url


def get_previous_season_url(starting_url):
    """
    From given current/starting results page for a given
    league, get the URL for the previous years results page
    to allow us to iterate backwards through seasons.
    Args:
        starting_url:

    Returns:

    """
    req = requests.get(starting_url)
    soup = bs(req.text, "html.parser")
    links = soup.findAll("a", href=True, text="Previous Season")
    previous_season_link = links[0]["href"]
    return f"{BASE_URL}{previous_season_link}"


def create_season_code(league_name, year):
    if league_name == "Major-League-Soccer":
        return str(year)
    else:
        return f"{year-1}-{year}"


def scrape_team_lineups():
    """
    For each league, season, iterate through each
    fbref.com match report page to extract the team
    lineups and manager information. This raw data
    is outputed to a SQS queue for a consumer/loader
    app to process later.

    Returns:

    """

    sqs = boto3.client('sqs')
    scraped_links = s3_utils.get_scraped_urls()

    queue_links = read_links_from_queue()

    for comp_no, league_name in COMPETITION_NUMBER_LEAGUE_MAP.items():
        competition_season_links = [
            (_generate_scores_url(comp_no, year, league_name), league_name, create_season_code(league_name, year))
            for year in range(2024, 2013, -1)
        ]

        print(f"League: {league_name}")

        for season_link in competition_season_links:
            season_object = FBrefSeasonResultsPage(season_link[0])
            league = season_link[1]
            season_code = season_link[2]

            print(f"Season: {season_link}")
            # We don't want to re-scrape links that are already in the message queue OR in the list of already processed links
            match_links_to_scrape = [
                link for link in season_object.match_level_urls if link not in scraped_links and link not in queue_links
            ]

            for match_link in match_links_to_scrape:
                try:
                    match_info = extract_lineup_manaager_info(match_link)
                    match_info['league'] = league
                    match_info['season_code'] = season_code

                    # Write the dict to the buffer file
                    message_json = json.dumps(match_info)
                    # Send the message to the queue
                    response = sqs.send_message(
                        QueueUrl=QUEUE_URL,
                        MessageBody=message_json,
                    )
                    print("MessageId:", response['MessageId'])
                    print(f"Message body: \n\n{message_json}")
                    s3_utils.update_scraped_url_list('football-misc', 'scraped_links.txt', [match_link])
                    print(f"Pushed {match_link} to queue.")
                except Exception as e:  # TODO: fix this better later
                    print("Exception:")
                    print(e)
    return


def extract_lineup_manaager_info(match_link: str) -> dict:
    """
    Take the match report fbref.com URL and return
    a dict object with team lineups and manager details for the home,
    away teams.

    Args:
        match_link: URL for a given match report,
        e.g: https://fbref.com/en/matches/12251835/Nottingham-Forest-Brentford-November-5-2022-Premier-League
    Returns:
        info: dict containing the extracted players, manager details
    """
    # match_link =
    match_soup = bs(requests.get(match_link).text, 'lxml')
    time.sleep(3.1)
    table = match_soup.find_all("table")
    # all the tables in the html doc:
    all_match_tables = pd.read_html(str(table))

    regex_pattern = r"^[^\(]+(?= \()"
    match = re.search(regex_pattern, all_match_tables[0].columns[0])
    home_team = match.group(0) if match else ""
    match = re.search(regex_pattern, all_match_tables[1].columns[0])
    away_team = match.group(0) if match else ""

    home_lineup = all_match_tables[0][:11].iloc[:, 1].values.tolist()
    away_lineup = all_match_tables[1][:11].iloc[:, 1].values.tolist()

    scorebox = match_soup.findAll(attrs={"class": "scorebox"})
    scorebox_meta = match_soup.findAll(attrs={"class": "scorebox_meta"})
    date_str_str = scorebox_meta[0].div.span["data-venue-date"]
    home_manager = (
        scorebox[0]
        .findAll(attrs={"class": "datapoint"})[0]
        .text.replace("\xa0", " ")
        .replace("Manager: ", "")
    )
    away_manager = (
        scorebox[0]
        .findAll(attrs={"class": "datapoint"})[2]
        .text.replace("\xa0", " ")
        .replace("Manager: ", "")
    )

    info = {}
    info['match_link'] = match_link
    info["date"] = date_str_str
    info["home_team"] = home_team
    info["away_team"] = away_team
    info["home_lineup"] = home_lineup
    info["away_lineup"] = away_lineup
    info["home_manager"] = home_manager
    info["away_manager"] = away_manager

    return info


def convert_to_df(message_body: dict) -> pd.DataFrame:
    """
    Process the lineups/managers message payload into
    tabular dataframe object.

    Args:
        message: dict object from sqs.receive_message

    Returns:
        staging_df: DataFrame of lineups, managers in 'long' format
    """

    staging_df = pd.DataFrame(zip(message_body['home_lineup'], message_body['away_lineup']), columns=['home_player', 'away_player'])
    staging_df['home_manager'] = message_body['home_manager']
    staging_df['away_manager'] = message_body['away_manager']
    staging_df['date'] = message_body['date']
    staging_df['home_team'] = message_body['home_team']
    staging_df['away_team'] = message_body['away_team']
    staging_df['league'] = message_body['league']
    staging_df['season_code'] = message_body['season_code']

    return staging_df


def read_links_from_queue():

    links = []
    sqs = boto3.client('sqs')

    while True:
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=10,  # Maximum is 10
            WaitTimeSeconds=20  # Enable long polling
        )

        # Check if any messages are returned
        if 'Messages' in response:
            for message in response['Messages']:
                # Process the message here...
                message_body = json.loads(message['Body'])
                links.append(message_body['match_link'])
        else:
            # No messages to process, break the loop
            break

    return links


def scrape_xg_result_seasons(league_name: str = None):
    #TODO: NEED TO LOOK AT AN UPDATE/BACKFILL CHOICE SIMILAR TO THE RESULTS LOADER
    """
    Load all seasons with xg results for given league.

    Given league name and the URL containing the current seasons
    scores/fixtures, process and load results from fbref.com into
    sqlite database table.
    Args:
        league_name:
        starting_url:

    Returns:

    """
    scraped_links = s3_utils.get_scraped_urls()

    all_league_links = []
    # Take advantage of the nice regularity in the fbref link structure
    for comp_no, league_name in COMPETITION_NUMBER_LEAGUE_MAP.items():
        links = [
            _generate_scores_url(comp_no, year, league_name)
            for year in range(2023, 2023 - 10, -1)
        ]
        all_league_links = all_league_links + links

    all_links_to_scrape = [
        link for link in all_league_links if link not in scraped_links
    ]

    for link in all_links_to_scrape:
        try:
            print(f"Scraping: {link}")
            fb_ref_season = FBrefSeasonResultsPage(link)
            fb_ref_season.save_to_s3()
        except Exception as e:  # TODO: need to log this
            print(e)
    return


def team_lineups_loader_handler(event, context):
    """
    AWS Lambda handler func to consume team lineup messages
    from the SQS queue, and upload the tabular data format to s3.
    Args:
        event:
        context:

    Returns:

    """
    for record in event['Records']:
        message = record["body"]
        print(f"printing message: {message}\n\n")
        message = json.loads(str(message))
        df = convert_to_df(message)
        league = df['league'].values[0]
        season_code = df['season_code'].values[0]
        date = df['date'].values[0]
        home = df['home_team'].values[0]
        away = df['away_team'].values[0]

        # Build the object key:
        file_key = f"{league}/{season_code}/{date}-{home}-{away}.csv"
        bucket = 'football-lineups-and-managers'
        s3_utils.upload_df_to_s3(bucket, file_key, df)
        print("Called upload_to_s3. Exiting")
    return


def scrape_xg_results_handler(event, context):
    version = os.environ['APP_VERSION']
    scrape_xg_result_seasons()

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"},
        "body": json.dumps({
            "Version ": version})}


def scrape_current_season_xg_results_handler(event, context):
    """
    Lambda function handler that scrapes the current season
    xg results for a given league and stores the results in
    the football-xg-results S3 bucket.
    Args:
        event:
        context:

    Returns:

    """
    for league, url in SCORES_HOME_PAGE_URLS.items():
        try:
            fb_ref_season = FBrefSeasonResultsPage(url)
        except Exception as e: # Some leagues may not have started (empty data can give rise to exceptions based on inferred types) 
            print(e)
            continue
        fb_ref_season.save_to_s3()
        time.sleep(3.2)

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"},
    }


#TODO: need to reorganise this module better

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


def standardise_current_xg_results_files_handler(event, context):
    """
    Lambda function handler that checks for missing or inconsistent
    columns, missing data handling before writing to (league, season)
    partitioned s3 buckets in the Parquet format
    Args:
        event:
        context:

    Returns:

    """
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    # Decoding the file key
    file_key = urllib.parse.unquote_plus(file_key)

    csv_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    body = csv_obj['Body'].read().decode('utf-8')

    df = pd.read_csv(io.StringIO(body))

    # Check if the certain string exists in the file name
    if 'round' not in df.columns.tolist():
        df['round'] = 'N/A'
    if 'week' not in df.columns.tolist():
        df['week'] = -1.0

    # we should partition by season and league name , in the folder structure
    league_name = df['league_name'].values[0]
    season_code = '-'.join([d for d in file_key.split('-') if d.isdigit() and len(d) == 4])  # i only want 'year' digits to be caught here

    df.drop(columns=['league_name'], inplace=True)
    df['attendance'] = df['attendance'].astype(float)

    df['time'] = df['time'].fillna('missing')
    df['venue'] = df['venue'].fillna('missing')

    # clean penalty shootout affected scorelines (from MLS)
    pattern = r'\([^)]*\)'
    df['away_goals'] = df['away_goals'].apply(lambda x: int(re.sub(pattern, '', x).strip()))
    df['home_goals'] = df['home_goals'].apply(lambda x: int(re.sub(pattern, '', x).strip()))

    output_columns = expected_columns
    output_buffer = io.BytesIO()

    df[output_columns].to_parquet(output_buffer, index=False)
    output_buffer.seek(0)

    # adopt the '=' in the folders to help Glue infer the partition key column names:
    s3.put_object(Bucket='football-xg-results-clean', Key=f"league={league_name}/season={season_code}/{file_key}.parquet",
                  Body=output_buffer.getvalue())


if __name__ == "__main__":
    valid_functions = ["xg", "lineups"]

    if len(sys.argv) < 2:
        print(f"Please provide a function name as an argument. Valid options are: {', '.join(valid_functions)}")
        sys.exit(1)

    func_name = sys.argv[1]

    if func_name == "xg":
        scrape_xg_result_seasons()
    elif func_name == "lineups":
        scrape_team_lineups()
    else:
        raise ValueError(f"Please provide one of {', '.join(valid_functions)} for the function name to call.")
