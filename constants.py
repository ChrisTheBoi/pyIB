import json
import os
import re
import pandas as pd
import requests
from tqdm.auto import tqdm
from user_agent import generate_user_agent

# prerequisites for requests
s = requests.Session()
random_ua = generate_user_agent()

heads = {'Host': 'www.sec.gov', 'Connection': 'close',
         'Accept': 'application/json, text/javascript, */*; q=0.01', 'X-Requested-With': 'XMLHttpRequest',
         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36',
         }

# the default filepath where a company's CIK and exchange are retrieved from
filepath = os.getcwd() + '/data/company_tickers.json'


def load_json(filepath=filepath):
    """for loading data from the default filepath, but, if necessary,
    also loads other json files"""

    try:
        data = pd.read_json(filepath)

    except ValueError:
        with open(filepath) as f:
            data = json.load(f)

    return data


def save_json(df):
    """for saving loaded data back to default filepath
    does not support storing any objects other than pd.DataFrame"""

    try:
        df.to_json(path_or_buf=filepath)

    except ValueError:
        pass

    return None


def get_cik_json(ticker):
    """retrieves company's CIK from default filepath"""

    with open(filepath) as f:
        data = json.load(f)

    for info in data.values():
        if info['ticker'] == ticker.upper():
            cik = info['cik_str']

            return cik


def get_exchange(ticker):
    """retrieves company's exchange from default filepath"""

    with open(filepath) as f:
        f = json.load(f)

        for key, value in f.items():
            if value['ticker'] == ticker.upper():
                exchange = value['exchange']

                return exchange

def download_master_index(year):
    for qtr in range(1, 5):
        url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
        response = requests.get(url, headers=heads)
        print(url)
        response.raise_for_status()
        down_direct = os.getcwd() + '/data/edgar_master_index'

        with open(f'{down_direct}/master{year}QTR{qtr}.idx', 'wb') as f:
            f.write(response.content)


def get_filings(ticker, form='10-K'):
    """scrapes master index files for enpoints
    these endpoints are used to download excel files of company financials
    provided by the SEC"""

    cik = get_cik_json(ticker)
    master_index = os.getcwd() + '/data/edgar_master_index/'
    directory = os.listdir(master_index)

    pattern = f'({form}).\d+.\d+.\d+.(edgar/data/{cik}/)(\d+.\d+.\d+)'

    downloads = []

    i = 0
    while i < len(directory):

        for file in directory:
            doc = master_index + file

            with open(doc, encoding='utf-8') as f:
                try:
                    regex = re.findall(pattern, f.read())

                    for item in regex:
                        downloads.append(item)

                except UnicodeDecodeError:
                    print(f'Failed to decode: {file}')
                    continue

    return downloads


def download_files(ticker, form='10-K'):
    """for downloading excel of company financials from SEC website"""

    files_names = get_filings(ticker, form=form)

    accession = []
    names = []

    for files_name in tqdm(files_names, bar_format='Appending names'):
        accession.append(files_name[-1])
        names.append(files_name[-2])

    formatted_accession = []

    for number in accession:
        number = accession.pop()
        x = number.replace('-', '')
        formatted_accession.append(x)

    formatted = []

    for number in formatted_accession:
        format = names[0] + number
        formatted.append(format)

    dstdir = os.getcwd() + f'/data/{ticker}_reports/'

    if not os.path.exists(dstdir):
        os.makedirs(dstdir)

    rename = f'{ticker}_report.xlsx'

    path = dstdir + rename

    for name in tqdm(formatted, bar_format='Downloading files'):

        i = 0
        while os.path.exists(path):
            rename = f'{ticker}_report_{i}.xlsx'
            path = dstdir + rename
            i += 1

        try:
            url = f'https://www.sec.gov/Archives/{name}/Financial_Report.xlsx'

            req = requests.get(url, headers=heads, stream=True)

            if req.status_code == 200:
                with open(path, 'wb') as f:
                    for chunk in req.iter_content(chunk_size=15000):
                        f.write(chunk)

            else:
                continue

        except Exception:
            continue

    return None


def get_income_statements(ticker):
    """imports downloaded excel files as pandas dataframes and appends them
    to list"""

    sheets = []

    path = os.getcwd() + f'/data/{ticker}_reports/'
    directory = os.listdir(path)

    for file in tqdm(directory):
        try:
            filename = path + file

            sheet_name = 'Consolidated_Statements_of_Inc'
            df = pd.read_excel(filename, sheet_name=sheet_name)
            sheets.append(df)

        except ValueError:
            try:
                filename = path + file
                sheet_name = 'Consolidated Statements of Inco'
                df = pd.read_excel(filename, sheet_name=sheet_name)
                sheets.append(df)
            except ValueError:
                continue

    return sheets


def get_balance_sheets(ticker):
    """imports downloaded excel files as pandas dataframes and appends them
    to list"""

    sheets = []

    path = os.getcwd() + f'/data/{ticker}_reports/'
    directory = os.listdir(path)

    for file in tqdm(directory):
        try:
            filename = path + file

            sheet_name = 'Consolidated_Balance_Sheets'
            df = pd.read_excel(filename, sheet_name=sheet_name)
            sheets.append(df)

        except ValueError:
            try:
                filename = path + file
                sheet_name = 'Consolidated Balance Sheets'
                df = pd.read_excel(filename, sheet_name=sheet_name)
                sheets.append(df)
            except ValueError:
                continue

    return sheets


def get_cash_flow_statements(ticker):
    """imports downloaded excel files as pandas dataframes and appends them
    to list"""

    sheets = []

    path = os.getcwd() + f'/data/{ticker}_reports/'
    directory = os.listdir(path)

    for file in tqdm(directory):
        try:
            filename = path + file

            sheet_name = 'Consolidated_Statements_of_Cas'
            df = pd.read_excel(filename, sheet_name=sheet_name)
            sheets.append(df)

        except ValueError:
            try:
                filename = path + file
                sheet_name = 'Consolidated Statements of Cas'
                df = pd.read_excel(filename, sheet_name=sheet_name)
                sheets.append(df)
            except ValueError:
                continue

    return sheets


def columns_to_years(df, inplace=False):
    """for changing column headers to the year of the statement and changes
    dataframes index"""

    try:
        pattern = r'\d{4}'
        dates = re.findall(pattern, str(df.loc[0]))

        headers = ['Accounts', ]
        for date in dates:
            headers.append(date)

        df.columns = headers

        try:
            df.set_index('Accounts', inplace=inplace)

        except IndexError:
            df

    except KeyError:
        pass

        return df


def convert_dtype(df):
    """converts column dtypes to int32"""

    for column in df.columns:
        if object in list(df[column]) != int(object):
            df.fillna(0)

    try:
        df.astype({column: 'int32'}).dtypes

    except Exception:
        pass

        return df
