"""
Module for handling the extraction and
processing of data from FBref.com.

Author: Padraig Cleary
"""

import datetime as dt
import json
import os
import re
import sys
import time

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
        self.output_file_name = self.link.split("/")[-1]
        self.processed_xg_df = self.preprocess_fbref_xg_results()
        self.match_level_urls = self.get_match_links_from_dom()

    def _get_soup_object(self) -> bs.BeautifulSoup:
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
        Save the processed season file to the football_pipeline-xg-results
        S3 bucket and update the S3 scraped_links file with the new link.
        Returns:
        """
        s3_utils.upload_df_to_s3('football_pipeline-xg-results', self.output_file_name+".csv", self.processed_xg_df)
        if not self.is_current_season:  # we always want to keep trying the current season link for freshly completed matches
            # so the current season NEVER gets inserted into the 'scraped season table' until new season starts.
            s3_utils.update_scraped_url_list('football_pipeline-misc', 'scraped_links.txt', [self.link])


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
            for year in range(2023, 2023 - 10, -1)
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
            QueueUrl='https://sqs.us-east-1.amazonaws.com/903546410703/team-linueps-queue',
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
        bucket = 'football_pipeline-lineups-and-managers'
        s3_utils.upload_df_to_s3(bucket, file_key, df)
        print("Called upload_to_s3. Exiting")
    return


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
