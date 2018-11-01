#!/usr/bin/env python3
"""
Download all the GRFN frames for Hawaii (path 87)

# Launch an EC2 in us-east-1 for free access to this S3 data!


NOTE: requires ~/.netrc file with earthdata login

download_GRFN.py 87
"""

from json import loads
from requests import get
from time import sleep
import os
import pandas as pd
import sys


path = sys.argv[1]
# Download to local disk
download_url = 'https://grfn.asf.alaska.edu/door/download/'
status_url = 'https://grfn.asf.alaska.edu/door/status/'
retry_interval_in_seconds = 30


def get_status(file_name):
    response = get(status_url + file_name)
    response.raise_for_status()
    return loads(response.text)['status']


def download_file(file_name):
    response = get(download_url + file_name, stream=True)
    response.raise_for_status()
    with open(os.path.basename(file_name), 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024*1024):
            if chunk:
                f.write(chunk)


def get_file(file_name):
    status = get_status(file_name)
    if status == 'archived':
        response = get(download_url + file_name)

    while status != 'available':
        print('status = {0}, sleeping'.format(status))
        sleep(retry_interval_in_seconds) # Just wait 1 minute to see if file is out of Glacier
        status = get_status(file_name)

    print('downloading {0}'.format(file_name))
    download_file(file_name)


def query_cmr(path):
    fmt = 'json'
    url = f'https://cmr.earthdata.nasa.gov/search/granules.{fmt}'
    params = {'collection_concept_id' : 'C1379535600-ASF',
              'temporal' : '2014-01-01T00:00:00Z', #single value is a start date
              'attribute[]' : f'int,PATH_NUMBER,{path}',
              'point' : '-155.287763,19.403492', #kilaeu
              'page_size' : 2000, #max number of results per page
             }

    r = get(url, params=params, timeout=100)
    print(r.url)
    df = pd.DataFrame(loads(r.text)['feed']['entry'])
    n = len(df)
    print(f'Found {n} interferograms for path {path}')

    return df


def print_summary(df):
    granule = df.producer_granule_id
    primary = pd.to_datetime(df.time_end).dt.round('D')
    secondary = pd.to_datetime(df.time_start).dt.round('D')
    dt = primary - secondary
    DF = pd.DataFrame(dict(dt=dt,
                           primary=primary,
                           secondary=secondary,
                           granule=granule))
    DF.sort_values(by=['primary','secondary'], inplace=True)
    DF.to_csv('summary.csv')
    print(DF)


def main(path):
    df = query_cmr(path)
    print_summary(df)

    # Get AWS temporary credentials before downloading
    credential_url = 'https://grfn.asf.alaska.edu/door/credentials'
    response = get(credential_url)
    response.raise_for_status()

    credentials = loads(response.text)['Credentials']
    print('Setting temporary AWS credentials for 1 hour:')
    print(credentials)
    os.environ['AWS_ACCESS_KEY_ID'] = credentials['AccessKeyId']
    os.environ['AWS_SECRET_ACCESS_KEY'] = credentials['SecretAccessKey']
    os.environ['AWS_SESSION_TOKEN'] = credentials['SessionToken']

    # Sort to download more recent first
    #df.sort_values('producer_granule_id', ascending=False, inplace=True)
    df.sort_values('time_start', ascending=False, inplace=True)

    for gid in df.producer_granule_id:
        file = gid + '.unw_geo.zip'
        print(f'\nDownloading {file}...\n')
        if os.path.isfile(file):
            print('File exists, skipping download')
        else:
            try:
                get_file(file)
            except Exception as e:
                print('Trouble downloading file!', e)


if __name__ == '__main__':
    path = sys.argv[1]
    main(path)
