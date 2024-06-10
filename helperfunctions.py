import pandas as pd
import requests
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import calendar
from bs4 import BeautifulSoup
from selenium import webdriver
import time
import numpy as np
import pycountry
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    df=df[['id','label']]
    epg_df = df.rename(columns={'id':'org_id',"label":"org_label"})
    return epg_df
#%%
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
    df=df[['id','label']]
    party_df = df.rename(columns={'id':'org_id',"label":"org_label"})
    return party_df
#%%
def generate_ep_df(ep_number):
    # Create a list of org_ids and org_labels based on the ep_number
    org_ids = [f'org/ep-{i}' for i in range(0, ep_number + 1)]
    org_labels = [f'EP{i}' for i in range(0, ep_number + 1)]

    # Create a DataFrame from the generated lists
    ep_df = pd.DataFrame({'org_id': org_ids, 'org_label': org_labels})
    return ep_df
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
        df = pd.json_normalize(data['data'], record_path=['hasMembership'],meta=['citizenship','bday','hasGender'])
    except ValueError as e:
        print(f"Error parsing JSON data: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on JSON parsing error
    df1 = df[(df['membershipClassification'].isnull()) & (df['role']=="def/ep-roles/MEMBER_PARLIAMENT")]
    df2 = df[df['membershipClassification'].isin(["def/ep-entities/EU_POLITICAL_GROUP","def/ep-entities/NATIONAL_CHAMBER"])]
    df = pd.concat([df1,df2])
    try:
        df = df[['organization','membershipClassification','memberDuring.startDate','memberDuring.endDate','citizenship','bday','hasGender']].copy()
    except KeyError as e:
        df = df[['organization','membershipClassification','memberDuring.startDate','citizenship','bday','hasGender']].copy()
        df.loc[:, 'memberDuring.endDate'] = np.NaN
    df.loc[:,'identifier'] = identifier
    return df
def get_memberships_df(mep_df, org_df):
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(get_membership, identifier) for identifier in mep_df.MepId]
        results = [future.result() for future in as_completed(futures)]

    memberships_df = pd.concat(results, ignore_index=True)
    memberships_df.rename(columns={'organization': 'org_id'}, inplace=True)
    memberships_df = pd.merge(memberships_df, org_df, on='org_id', how='left')
    return memberships_df

def extract_birthday(identifier,df):
    df = df[df['identifier'].astype(str) == str(identifier)]
    bday = df.iloc[0]["bday"]
    return bday
def extract_gender(identifier,df):
    df = df[df['identifier'].astype(str) == str(identifier)]
    hasGender = df.iloc[0]['hasGender']
    parts = hasGender.rsplit('/', 1)
    return parts[-1] if len(parts) > 1 else hasGender
def get_mep_database(mep_df,memberships_df):
    temp_df = mep_df.copy()
    mep_df = mep_df.copy()
    mep_df['MepId'] = temp_df['MepId']
    mep_df['Fname'] = temp_df['Fname']
    mep_df['Lname'] = temp_df['Lname']
    mep_df['FullName'] = temp_df['FullName']
    mep_df['Birthday'] = temp_df['MepId'].apply(extract_birthday,df=memberships_df)
    mep_df['Gender'] = temp_df['MepId'].apply(extract_gender,df=memberships_df)
    mep_df['Country'] = temp_df['Country']
    return mep_df