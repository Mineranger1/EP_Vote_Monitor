import pandas as pd
import requests
import re
import subprocess
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import calendar
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options as ChromeOptions
from tempfile import mkdtemp
import time
import numpy as np
import pycountry
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from io import StringIO
from sqlalchemy import create_engine


def get_meetings(year, month):
    url = f'https://data.europarl.europa.eu/api/v2/meetings?year={year}&format=application%2Fld%2Bjson&offset=0'
    try:
        # Fetch data from the URL
        response = requests.get(url)

        # Check if the response was successful
        if response.status_code == 200:
            # Parse the JSON data
            data = response.json()
            df = pd.json_normalize(data['data'])
        elif response.status_code == 204:
            print("No content available for this request" + url)
            return pd.DataFrame()  # Return an empty DataFrame for no content

        elif response.status_code == 504:
            # Handle 504 Gateway Timeout specifically with a retry
            print("Gateway Timeout (504) encountered. Retrying..." + url)

        response.raise_for_status()

    except requests.RequestException as e:
        print(f"Error fetching data from URL: {url} - {e}")
        return pd.DataFrame()

    def extract_date(meeting):
        # Wait for 2 seconds before retrying
        substring_to_remove = "MTG-PL-"
        # Safely handle None values
        safe_meeting = meeting or ""

        if substring_to_remove in meeting:
            result_string = meeting.replace(substring_to_remove, "")
            return result_string
        else:
            return safe_meeting

    df['Date'] = pd.to_datetime(df['activity_id'].apply(extract_date))

    df = df[df['Date'].dt.month == month]

    return df


def get_xml(date, ep_number):
    # Format the URL
    url = f'https://www.europarl.europa.eu/doceo/document/PV-{ep_number}-{date}-VOT_EN.xml'

    try:
        # Fetch the XML content from the URL
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for non-200 status codes
    except requests.RequestException as e:
        print(f"Error fetching data from URL: {url} - {e}")
        return pd.DataFrame()  # Return an empty DataFrame if there's a request error

    try:
        # Parse the XML content
        root = ET.fromstring(response.text)
    except ET.ParseError as e:
        print(f"Error parsing XML data: {e}")
        return pd.DataFrame()  # Return an empty DataFrame if parsing fails

    # Prepare a list to hold rows of data
    data = []

    # Find all votes
    for vote in root.findall('.//vote'):
        title = vote.find('title').text if vote.find('title') is not None else None
        label = vote.find('label').text if vote.find('label') is not None else None
        vote_committee = vote.attrib.get('committee', None)
        votings = vote.findall('.//voting')

        # Find all votings under each vote
        for i, voting in enumerate(votings):
            # Extract nested text elements
            voting_title = voting.find('title').text if voting.find('title') is not None else None
            voting_label = voting.find('label').text if voting.find('label') is not None else None
            amendment_subject = voting.find('amendmentSubject').text if voting.find(
                'amendmentSubject') is not None else None
            amendment_number = voting.find('amendmentNumber').text if voting.find(
                'amendmentNumber') is not None else None
            amendment_author = voting.find('amendmentAuthor').text if voting.find(
                'amendmentAuthor') is not None else None
            final_vote = (i == len(votings) - 1)

            # Add all required fields to the row
            row = {
                'vote_title': title,
                'vote_label': label,
                'vote_committee': vote_committee,
                'voting_id': voting.attrib.get('votingId', None),
                'result': voting.attrib.get('result', None),
                'result_type': voting.attrib.get('resultType', None),
                'voting_title': voting_title,
                'voting_label': voting_label,
                'amendment_subject': amendment_subject,
                'amendment_number': amendment_number,
                'amendment_author': amendment_author,
                'final_vote': final_vote
            }
            data.append(row)
    df = pd.DataFrame(data)
    # Create and return a DataFrame from the data
    return df[df['result_type'] == "ROLL_CALL"].reset_index(drop=True)


def get_api(date):
    url = f'https://data.europarl.europa.eu/api/v2/meetings/MTG-PL-{date}/decisions?vote-method=ROLL_CALL_EV&format=application%2Fld%2Bjson&json-layout=framed&limit=5000'
    try:
        # Fetch data from the URL
        response = requests.get(url)

        # Check for HTTP 204 (No Content)
        if response.status_code == 204:
            print("No content available for this request " + url)
            return pd.DataFrame()  # Return an empty DataFrame for no content
        elif response.status_code == 504:
            # Handle 504 Gateway Timeout specifically with a retry
            print("Gateway Timeout (504) encountered." + url)
            return pd.DataFrame()

        # Raise an error for other non-success status codes
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching data from URL: {url} - {e}")
        return pd.DataFrame()  # Return an empty DataFrame on request error

    try:
        # Parse the JSON data
        data = response.json()
        df = pd.json_normalize(data['data'])
    except ValueError as e:
        print(f"Error parsing JSON data: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on JSON parsing error

    return df


def get_meeting(date):
    url = f'https://data.europarl.europa.eu/api/v2/meetings/MTG-PL-{date}?format=application%2Fld%2Bjson&language=en'
    try:
        # Fetch data from the URL
        response = requests.get(url)

        # Check if the response was successful
        if response.status_code == 200:
            # Parse the JSON data
            data = response.json()
            df = pd.json_normalize(data['data'])
            return df

        elif response.status_code == 204:
            print("No content available for this request" + url)
            return pd.DataFrame()  # Return an empty DataFrame for no content

        elif response.status_code == 504:
            # Handle 504 Gateway Timeout specifically with a retry
            print("Gateway Timeout (504) encountered." + url)
            return pd.DataFrame()
        else:
            # Raise an exception for other non-successful status codes
            response.raise_for_status()

    except requests.RequestException as e:
        print(f"Error fetching data from URL: {url} - {e}")
        return pd.DataFrame()


def get_data_for_date(date_str, ep_number):
    # Calls all individual data-fetch functions for a given date.
    api_data = get_api(date_str)
    xml_data = get_xml(date_str, ep_number)
    meeting_data = get_meeting(date_str)
    return [api_data, xml_data, meeting_data]


def get_raw_data_for_month(year, month, ep_number):
    meetings_df = get_meetings(year, month)
    date_strs = meetings_df['Date'].astype(str).tolist()
    results = []
    for date_str in date_strs:
        result = get_data_for_date(date_str, ep_number)
        results.append(result)

    # Unpacking results
    api_results = [result[0] for result in results]
    xml_results = [result[1] for result in results]
    meeting_results = [result[2] for result in results]

    # Concatenate all results into dataframes
    api_df = pd.concat(api_results, ignore_index=True)
    xml_df = pd.concat(xml_results, ignore_index=True)
    meeting_df = pd.concat(meeting_results, ignore_index=True)

    # Specific filtering or restructuring can be done here if needed
    meeting_df = meeting_df[['activity_date', 'had_excused_person', 'had_participant_person']]
    api_df = api_df  # Assuming some filter list `api_filtered_list` if needed

    return api_df, xml_df, meeting_df


def extract_procedure(title_string):
    # Find the position of the first asterisk
    asterisk_index = title_string.find('*')

    # If an asterisk was found, return the substring starting from that index
    if asterisk_index != -1:
        return title_string[asterisk_index:]
    else:
        return None  # Return an empty string if no asterisk is found


def extract_leg(row):
    substring = "budget"
    # Check if 'Procedure' is not None
    if row['Procedure'] is not None:
        return "Leg"

    # Safely check for 'substring' in 'vote_title' and 'vote_committee'
    title_contains_substring = substring in (row['vote_title'] or "").lower()
    committee_contains_substring = substring in (row['vote_committee'] or "").lower()

    # Determine the return value based on the conditions
    if title_contains_substring or committee_contains_substring:
        return "Bud"
    else:
        return "Non-Leg"


def extract_report(report_string):
    try:
        # Check if the input is not empty and is a string
        if not isinstance(report_string, str) or not report_string:
            return None

        prefix = "Report: "
        start_idx = len(prefix)

        # Check if the string starts with the expected prefix
        if not report_string.startswith(prefix):
            return None

        # Remove the prefix
        without_prefix = report_string[start_idx:]

        # Find the start of the details in parentheses
        if " (" not in without_prefix:
            return None

        # Extract the name and surname
        name_and_surname = without_prefix.split(" (")[0]

        return name_and_surname

    except ValueError as e:
        return None


def generate_url(vote_label):
    pattern = r"([ABC])(\d)-(\d{4})/(\d{4})"
    if vote_label is not None:
        match = re.search(pattern, vote_label)
        if match:
            letter, EPNumber, numbers1, numbers2 = match.groups()
            url = f"https://www.europarl.europa.eu/doceo/document/{letter}-{EPNumber}-{numbers2}-{numbers1}_EN.html"
            return url
        else:
            return None
    else:
        return None


def extract_committee(vote_committee):
    # Substring to remove
    substring_to_remove = "Committee: "
    # Safely handle None values
    safe_vote_committee = vote_committee or ""
    if substring_to_remove in safe_vote_committee:
        result_string = safe_vote_committee.replace(substring_to_remove, "")
        return result_string
    else:
        return safe_vote_committee  # Return the input or an empty string if None was input


def extract_policy_area(vote_committee):
    # Substrings to remove
    substring_to_remove = "Committee on "
    substring_to_remove2 = "the "
    # Safely handle None values
    safe_vote_committee = vote_committee or ""
    if substring_to_remove in safe_vote_committee:
        result_string = safe_vote_committee.replace(substring_to_remove, "")
        if substring_to_remove2 in result_string:
            result_string = result_string.replace(substring_to_remove2, "")

        return result_string
    else:
        return safe_vote_committee  # Return the input or an empty string if None was input


def get_votings_for_app_v1(api_df, xml_df):
    pd.set_option('future.no_silent_downcasting', True)
    api_df.rename(columns={'notation_votingId': 'voting_id'}, inplace=True)
    votings_df = pd.DataFrame()
    temp_df = pd.merge(xml_df, api_df, on='voting_id', how='left')
    votings_df["VoteId"] = temp_df.voting_id
    votings_df["Date"] = pd.to_datetime(temp_df.activity_date)
    votings_df["Title"] = temp_df['vote_title']
    temp_df["Procedure"] = temp_df.vote_title.apply(extract_procedure)
    votings_df["Procedure"] = temp_df["Procedure"]
    votings_df["Leg/Non-Leg/Bud"] = temp_df.apply(extract_leg, axis=1)
    votings_df["TypeOfVote"] = temp_df['voting_title']
    votings_df["VotingRule"] = "s"
    votings_df["Rapporteur"] = temp_df['vote_label'].apply(extract_report)
    votings_df["Link"] = temp_df['vote_label'].apply(generate_url)
    temp_df['CommitteeResponsabile'] = temp_df['vote_committee'].apply(extract_committee)
    votings_df["CommitteeResponsabile"] = temp_df["CommitteeResponsabile"]
    votings_df['PolicyArea'] = temp_df['CommitteeResponsabile'].apply(extract_policy_area)
    votings_df['Subject'] = temp_df['amendment_subject']
    votings_df['FinalVote'] = temp_df['final_vote'].infer_objects(copy=False).replace({True: 1, False: 0})
    votings_df['AmNo'] = temp_df['amendment_number']
    votings_df['Author'] = temp_df['amendment_author']
    votings_df['Vote'] = temp_df['had_decision_outcome'].infer_objects(copy=False).replace(
        {"def/ep-statuses/ADOPTED": 1, 'def/ep-statuses/REJECTED': 0})
    votings_df['Yes'] = temp_df['number_of_votes_favor'].astype("Int64")
    votings_df['No'] = temp_df['number_of_votes_against'].astype("Int64")
    votings_df['Abs'] = temp_df['number_of_votes_abstention'].astype("Int64")

    return votings_df


def get_votings_for_database(api_df, xml_df):
    pd.set_option('future.no_silent_downcasting', True)
    votings_df = pd.DataFrame()
    temp_df = pd.merge(xml_df, api_df, on='voting_id', how='left')
    votings_df["VoteId"] = temp_df.voting_id.astype("Int64")
    votings_df["Date"] = pd.to_datetime(temp_df.activity_date).astype("datetime64[ns]")
    votings_df["Title"] = temp_df['vote_title'].astype("str")
    votings_df["TypeOfVote"] = temp_df['voting_title'].astype('str')
    votings_df["Rapporteur"] = temp_df['vote_label'].apply(extract_report).astype("str")
    votings_df["Link"] = temp_df['vote_label'].apply(generate_url).astype("str")
    temp_df['CommitteeResponsabile'] = temp_df['vote_committee'].apply(extract_committee).astype("str")
    votings_df["CommitteeResponsabile"] = temp_df["CommitteeResponsabile"].astype("str")
    votings_df['Subject'] = temp_df['amendment_subject'].astype('str')
    votings_df['FinalVote'] = temp_df['final_vote'].infer_objects(copy=False).replace({True: 1, False: 0}).astype(
        "Int64")
    votings_df['AmNo'] = temp_df['amendment_number'].astype('str')
    votings_df['Author'] = temp_df['amendment_author'].astype("str")
    votings_df['Vote'] = temp_df['had_decision_outcome'].infer_objects(copy=False).replace(
        {"def/ep-statuses/ADOPTED": 1, 'def/ep-statuses/REJECTED': 0}).astype("Int64")
    votings_df['Yes'] = temp_df['number_of_votes_favor'].astype("Int64")
    votings_df['No'] = temp_df['number_of_votes_against'].astype("Int64")
    votings_df['Abs'] = temp_df['number_of_votes_abstention'].astype("Int64")
    return votings_df


def get_epgs():
    url = 'https://data.europarl.europa.eu/api/v2/corporate-bodies?body-classification=EU_POLITICAL_GROUP&format=application%2Fld%2Bjson&offset=0'
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from URL: {url} - {e}")
        return pd.DataFrame()
    try:
        df = pd.json_normalize(data['data'])
    except ValueError as e:
        print(f"Error parsing JSON data: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on JSON parsing error
    df = df[['id', 'label']]
    epg_df = df.rename(columns={'id': 'org_id', "label": "org_label"})
    return epg_df


def get_parties():
    url = 'https://data.europarl.europa.eu/api/v2/corporate-bodies?body-classification=NATIONAL_CHAMBER&format=application%2Fld%2Bjson&offset=0'
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from URL: {url} - {e}")
        return pd.DataFrame()
    try:
        df = pd.json_normalize(data['data'])
    except ValueError as e:
        print(f"Error parsing JSON data: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on JSON parsing error
    df = df[['id', 'label']]
    party_df = df.rename(columns={'id': 'org_id', "label": "org_label"})
    return party_df


def generate_ep_df(ep_number):
    # Create a list of org_ids and org_labels based on the ep_number
    org_ids = [f'org/ep-{i}' for i in range(0, ep_number + 1)]
    org_labels = [f'EP{i}' for i in range(0, ep_number + 1)]

    # Create a DataFrame from the generated lists
    ep_df = pd.DataFrame({'org_id': org_ids, 'org_label': org_labels})
    return ep_df


def get_mep_data(ep_number):
    url = f'https://data.europarl.europa.eu/api/v2/meps?parliamentary-term={ep_number}&format=application%2Fld%2Bjson&offset=0'
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from URL: {url} - {e}")
        return pd.DataFrame()
    try:
        mep_df = pd.json_normalize(data['data'])
    except ValueError as e:
        print(f"Error parsing JSON data: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on JSON parsing error
    return mep_df


def get_membership(identifier):
    url = f"https://data.europarl.europa.eu/api/v2/meps/{identifier}?format=application%2Fld%2Bjson"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from URL: {url} - {e}")
        return pd.DataFrame()
    try:
        df = pd.json_normalize(data['data'], record_path=['hasMembership'], meta=['citizenship', 'bday', 'hasGender'])
    except ValueError as e:
        print(f"Error parsing JSON data: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on JSON parsing error
    df1 = df[(df['membershipClassification'].isnull()) & (df['role'] == "def/ep-roles/MEMBER_PARLIAMENT")]
    df2 = df[
        df['membershipClassification'].isin(["def/ep-entities/EU_POLITICAL_GROUP", "def/ep-entities/NATIONAL_CHAMBER"])]
    df = pd.concat([df1, df2])
    try:
        df = df[['organization', 'membershipClassification', 'memberDuring.startDate', 'memberDuring.endDate',
                 'citizenship', 'bday', 'hasGender']].copy()
    except KeyError as e:
        df = df[['organization', 'membershipClassification', 'memberDuring.startDate', 'citizenship', 'bday',
                 'hasGender']].copy()
        df.loc[:, 'memberDuring.endDate'] = np.NaN
    df.loc[:, 'identifier'] = identifier
    return df


def get_memberships_df(mep_df, org_df):
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(get_membership, identifier) for identifier in mep_df['identifier']]
        results = [future.result() for future in as_completed(futures)]

    memberships_df = pd.concat(results, ignore_index=True)
    memberships_df.rename(columns={'organization': 'org_id'}, inplace=True)
    memberships_df = pd.merge(memberships_df, org_df, on='org_id', how='left')
    return memberships_df


def extract_memberships_info(identifier, df, date, ep_number):
    # Convert string date to datetime if not already in datetime format
    if not isinstance(date, pd.Timestamp):
        date = pd.to_datetime(date)

    # Convert start and end dates to datetime

    df = df[df['identifier'].astype(str) == str(identifier)]
    # Filter the DataFrame to find the record for the member on the given date
    party_info = df[
        (df['membershipClassification'].astype(str) == "def/ep-entities/NATIONAL_CHAMBER") &
        (df['memberDuring.startDate'] <= date) &
        ((df['memberDuring.endDate'].isna()) | (df['memberDuring.endDate'] >= date))
        ]
    epg_info = df[
        (df['membershipClassification'].astype(str) == "def/ep-entities/EU_POLITICAL_GROUP") &
        (df['memberDuring.startDate'] <= date) &
        ((df['memberDuring.endDate'].isna()) | (df['memberDuring.endDate'] >= date))
        ]
    ep_info = df[
        (df['org_id'] == f"org/ep-{ep_number}") &
        (df['memberDuring.startDate'] <= date) &
        ((df['memberDuring.endDate'].isna()) | (df['memberDuring.endDate'] >= date))
        ]
    start_end_info = df[
        df['org_id'] == f"org/ep-{ep_number}"
        ]
    # Return the organization label if a matching record is found, else None
    if not party_info.empty:
        party = party_info.iloc[0]['org_label']
    else:
        party = np.NaN
    if not epg_info.empty:
        epg = epg_info.iloc[0]['org_label']
    else:
        epg = np.NaN
    if not ep_info.empty:
        activ = "yes"
    else:
        activ = "no"
    country_url = df.iloc[0]['citizenship']
    country = pycountry.countries.get(alpha_3=(country_url.split('/')[-1])).name
    try:
        start = pd.to_datetime(start_end_info.iloc[0]['memberDuring.startDate'])
        end = pd.to_datetime(start_end_info.iloc[0]['memberDuring.endDate'])
    except IndexError as e:
        start = np.NaN
        end = np.NaN
    return activ, country, party, epg, start, end


def extract_birthday(identifier, df):
    df = df[df['identifier'].astype(str) == str(identifier)]
    bday = df.iloc[0]["bday"]
    return bday


def extract_gender(identifier, df):
    df = df[df['identifier'].astype(str) == str(identifier)]
    hasGender = df.iloc[0]['hasGender']
    parts = hasGender.rsplit('/', 1)
    return parts[-1] if len(parts) > 1 else hasGender


def initialise_driver():
    chrome_options = ChromeOptions()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-dev-tools")
    chrome_options.add_argument("--no-zygote")
    # chrome_options.add_argument("--single-process")
    chrome_options.add_argument("--remote-debugging-pipe")
    chrome_options.add_argument("--verbose")
    chrome_options.add_argument('--no-proxy-server')
    chrome_options.binary_location = "/opt/chrome/chrome-linux64/chrome"

    service = Service(
        executable_path="/opt/chrome-driver/chromedriver-linux64/chromedriver",
        service_args=['--log-level=INFO'],
        log_output=subprocess.STDOUT
    )

    driver = webdriver.Chrome(
        service=service,
        options=chrome_options
    )
    return driver


def get_seat_ids_web():
    url = 'https://www.europarl.europa.eu/meps/en/search/chamber'
    driver = initialise_driver()
    # driver = webdriver.Chrome()
    driver.get(url)
    time.sleep(5)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    circles = soup.find_all('circle')
    data = []
    for circle in circles:
        circle_data = {
            'id': circle.get('id'),
            'data-id-mep': circle.get('data-id-mep'),

        }
        data.append(circle_data)

    seat_id_df = pd.DataFrame(data)
    seat_id_df.rename(columns={'id': 'SeatId', 'data-id-mep': 'MepId'}, inplace=True)

    return seat_id_df


def categorize_vote_app(mep_id, vote_info, not_mep_df):
    # Helper function to check if a list contains only valid (non-np.NaN) data
    def is_valid_voter_list(voter_list):
        if isinstance(voter_list, list) and not any(
                np.isnan(item) if isinstance(item, float) else False for item in voter_list):
            return True
        return False

    # Check each voting category explicitly, ensuring data is valid
    voter_list = vote_info.get('had_voter_favor', [])
    if is_valid_voter_list(voter_list) and mep_id in voter_list:
        return 1

    voter_list = vote_info.get('had_voter_against', [])
    if is_valid_voter_list(voter_list) and mep_id in voter_list:
        return 2

    voter_list = vote_info.get('had_voter_abstention', [])
    if is_valid_voter_list(voter_list) and mep_id in voter_list:
        return 3

    voter_list = vote_info.get('had_voter_intended_favor', [])
    if is_valid_voter_list(voter_list) and mep_id in voter_list:
        return 1

    voter_list = vote_info.get('had_voter_intended_against', [])
    if is_valid_voter_list(voter_list) and mep_id in voter_list:
        return 2

    voter_list = vote_info.get('had_voter_intended_abstention', [])
    if is_valid_voter_list(voter_list) and mep_id in voter_list:
        return 3

    voter_list = vote_info.get('had_excused_person', [])
    if is_valid_voter_list(voter_list) and mep_id in voter_list:
        return 4

    voter_list = vote_info.get('had_participant_person', [])
    if is_valid_voter_list(voter_list) and mep_id in voter_list:
        return 5
    if mep_id in not_mep_df.values:
        return 0
    else:
        return 4


def get_activity_status(mep_id, df, date, ep_number):
    ep_info = df[
        (df['identifier'].astype(str) == str(mep_id)) &
        (df['org_id'] == f"org/ep-{ep_number}") &
        (df['memberDuring.startDate'] <= date) &
        ((df['memberDuring.endDate'].isna()) | (df['memberDuring.endDate'] >= date))
        ]
    return "yes" if not ep_info.empty else "no"


def get_country(mep_id, df):
    country_url = df.loc[df['identifier'].astype(str) == str(mep_id), 'citizenship'].iloc[0]
    country_code = country_url.split('/')[-1]
    return pycountry.countries.get(alpha_3=country_code).name if country_code else np.NaN


def get_party(mep_id, df, date):
    party_info = df[
        (df['identifier'].astype(str) == str(mep_id)) &
        (df['membershipClassification'] == "def/ep-entities/NATIONAL_CHAMBER") &
        (df['memberDuring.startDate'] <= date) &
        ((df['memberDuring.endDate'].isna()) | (df['memberDuring.endDate'] >= date))
        ]
    return party_info['org_label'].iloc[0] if not party_info.empty else np.NaN


def get_epg(mep_id, df, date):
    epg_info = df[
        (df['identifier'].astype(str) == str(mep_id)) &
        (df['membershipClassification'] == "def/ep-entities/EU_POLITICAL_GROUP") &
        (df['memberDuring.startDate'] <= date) &
        ((df['memberDuring.endDate'].isna()) | (df['memberDuring.endDate'] >= date))
        ]
    return epg_info['org_label'].iloc[0] if not epg_info.empty else np.NaN


def get_start_date(mep_id, df, ep_number):
    start_date_info = df[
        (df['identifier'].astype(str) == str(mep_id)) &
        (df['org_id'] == f"org/ep-{ep_number}")
        ]
    return start_date_info['memberDuring.startDate'].iloc[0] if not start_date_info.empty else np.NaN


def get_end_date(mep_id, df, ep_number):
    end_date_info = df[
        (df['identifier'].astype(str) == str(mep_id)) &
        (df['org_id'] == f"org/ep-{ep_number}")
        ]
    return end_date_info['memberDuring.endDate'].iloc[0] if not end_date_info.empty else np.NaN


def get_votes_df_for_app(memberships_df, mep_df, api_df, seat_id_df, meetings_df, ep_number):
    mep_df.rename(columns={'identifier': 'MepId'}, inplace=True)
    temp_df = pd.merge(mep_df, seat_id_df, on="MepId", how='left')
    date = api_df.iloc[0]['activity_date']
    votes_df = pd.DataFrame()
    votes_df['MepId'] = temp_df['MepId']
    votes_df['SeatId'] = temp_df['SeatId']
    votes_df['Fname'] = temp_df['givenName']
    votes_df['Lname'] = temp_df['familyName']
    votes_df['FullName'] = temp_df['label']
    votes_df['Activ'] = temp_df['MepId'].apply(get_activity_status, df=memberships_df, date=date, ep_number=ep_number)
    votes_df['Country'] = temp_df['MepId'].apply(get_country, df=memberships_df)
    votes_df['Party'] = temp_df['MepId'].apply(get_party, df=memberships_df, date=date)
    votes_df['EPG'] = temp_df['MepId'].apply(get_epg, df=memberships_df, date=date)
    votes_df['Start'] = temp_df['MepId'].apply(get_start_date, df=memberships_df, ep_number=ep_number)
    temp_df['End'] = temp_df['MepId'].apply(get_end_date, df=memberships_df, ep_number=ep_number)
    votes_df['End'] = temp_df['End']
    temp_api_df = pd.merge(api_df, meetings_df, on='activity_date', how="left")
    not_mep_df = temp_df[pd.notna(temp_df['End'])].id
    new_data = {}
    for i, row in temp_api_df.iterrows():
        voting_id = row['voting_id']
        vote_info = {
            'had_voter_favor': row['had_voter_favor'],
            'had_voter_against': row['had_voter_against'],
            'had_voter_abstention': row['had_voter_abstention'],
            'had_voter_intended_favor': row['had_voter_intended_favor'],
            'had_voter_intended_against': row['had_voter_intended_against'],
            'had_voter_intended_abstention': row['had_voter_intended_abstention'],
            'had_participant_person': row['had_participant_person'],
            'had_excused_person': row['had_excused_person']
        }

        # Apply 'categorize_vote' for each MEP in 'temp_df'
        new_data[voting_id] = temp_df['id'].apply(lambda x: categorize_vote_app(x, vote_info, not_mep_df=not_mep_df))
    new_columns_df = pd.DataFrame(new_data)
    votes_df = pd.concat([votes_df, new_columns_df], axis=1)

    return votes_df


def get_mep_database(mep_df, memberships_df):
    temp_df = mep_df.copy()
    mep_df = mep_df.copy()
    mep_df['MepId'] = temp_df['MepId'].astype("Int64")
    mep_df['Fname'] = temp_df['givenName'].astype("str")
    mep_df['Lname'] = temp_df['familyName'].astype("str")
    mep_df['FullName'] = temp_df['label'].astype("str")
    mep_df['Birthday'] = temp_df['MepId'].apply(extract_birthday, df=memberships_df).astype("datetime64[ns]")
    mep_df['Gender'] = temp_df['MepId'].apply(extract_gender, df=memberships_df).astype("str")
    mep_df['Country'] = temp_df['MepId'].apply(get_country, df=memberships_df).astype("str")
    return mep_df


def get_votes_for_database(memberships_df, mep_df, api_df, meetings_df, ep_number):
    temp_df = mep_df.copy()
    temp_df['End'] = temp_df['MepId'].apply(get_end_date, df=memberships_df, ep_number=ep_number)
    temp_api_df = pd.merge(api_df, meetings_df, on='activity_date', how="left")
    not_mep_df = temp_df[pd.notna(temp_df['End'])].id
    new_data = []
    for i, row in temp_api_df.iterrows():
        voting_id = row['voting_id']
        vote_info = {
            'had_voter_favor': row['had_voter_favor'],
            'had_voter_against': row['had_voter_against'],
            'had_voter_abstention': row['had_voter_abstention'],
            'had_voter_intended_favor': row['had_voter_intended_favor'],
            'had_voter_intended_against': row['had_voter_intended_against'],
            'had_voter_intended_abstention': row['had_voter_intended_abstention'],
            'had_participant_person': row['had_participant_person'],
            'had_excused_person': row['had_excused_person']
        }

        for mep_id in temp_df['id']:
            outcome = categorize_vote_app(mep_id, vote_info, not_mep_df)
            new_data.append({
                'VoteId': voting_id,
                'MepId': mep_id,
                'Vote': outcome
            })

    votes_df = pd.DataFrame(new_data)
    votes_df['VoteId'] = votes_df['VoteId'].astype("Int64")
    votes_df['MepId'] = votes_df['MepId'].astype("Int64")
    votes_df['Vote'] = votes_df['Vote'].astype("Int64")
    return votes_df


def get_memberships_database(memberships_df):
    temp_df = memberships_df.copy()
    memberships_database = pd.DataFrame()
    memberships_database['MepId'] = temp_df['identifier'].astype("Int64")
    memberships_database['OrgId'] = temp_df['org_id'].astype('str')
    memberships_database['Classification'] = temp_df['membershipClassification'].astype('str')
    memberships_database['Start'] = temp_df['memberDuring.startDate'].astype('datetime64[ns]')
    memberships_database['End'] = temp_df['memberDuring.endDate'].astype('datetime64[ns]')
    memberships_database['Label'] = temp_df['org_label'].astype('str')
    return memberships_database


def upload_to_s3(file_content, bucket_name, object_name):
    # Upload the file
    s3_client = boto3.client('s3')
    try:
        s3_client.put_object(Body=file_content, Bucket=bucket_name, Key=object_name)
    except NoCredentialsError:
        print("Credentials not available")
        return False
    except PartialCredentialsError:
        print("Incomplete credentials provided")
        return False
    return True


def export_files_to_csv(votings_df, votes_df, year, month, bucket_name):
    formatted_month = f"{month:02d}"

    # Skip the month if there are no entries
    if votings_df.empty and votes_df.empty:
        return f"No data for {year}-{formatted_month}. Skipping..."

    # Create the folder path using the formatted month
    month_folder = os.path.join(str(year), formatted_month)

    # Define paths for the votes and votations CSV files using the formatted month
    votes_file_name = f"RCVs-{year}-{formatted_month}-votes.csv"
    votations_file_name = f"RCVs-{year}-{formatted_month}.csv"

    votes_object_name = os.path.join(month_folder, votes_file_name)
    votations_object_name = os.path.join(month_folder, votations_file_name)

    # Convert DataFrames to CSV in memory
    votes_csv_buffer = StringIO()
    votings_csv_buffer = StringIO()

    votes_df.to_csv(votes_csv_buffer, index=False, sep=';')
    votings_df.to_csv(votings_csv_buffer, index=False, sep=';')

    # Upload CSV to S3
    upload_success_votes = upload_to_s3(votes_csv_buffer.getvalue(), bucket_name, votes_object_name)
    upload_success_votations = upload_to_s3(votings_csv_buffer.getvalue(), bucket_name, votations_object_name)

    if upload_success_votes and upload_success_votations:
        print("CSV files have been uploaded to S3.")
        return True
    else:
        print("Failed to upload CSV files to S3.")
        return False


def post_to_sql(votes_database, votings_database, mep_database, memberships_database, ep_number):
    engine = create_engine(f'postgresql+psycopg2://username:password@host:port/EP{ep_number}')
    try:
        if votes_database.empty:
            raise ValueError("Votes dataframe is empty")
        votes_database.to_sql('Votes', engine, if_exists='append', index=False)
        if votings_database.empty:
            raise ValueError("Votings dataframe is empty")
        votings_database.to_sql('Votings', engine, if_exists='append', index=False)
        if mep_database.empty:
            raise ValueError("Mep dataframe is empty")
        mep_database.to_sql('Mep_info', engine, if_exists='replace', index=False)
        if memberships_database.empty:
            raise ValueError("Membership dataframe is empty")
        memberships_database.to_sql('Memberships', engine, if_exists='replace', index=False)
        return True
    except Exception as e:
        print(e)
        return False






