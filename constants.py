import json
import os
import re
import time
import numpy as np
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
filepath = str(Path(''.join([os.getcwd(),'/data/company_tickers.json'])))


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
    qtr = 1
    while qtr < 5:
        try:
            url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
            response = requests.get(url, headers=heads)
            response.raise_for_status()

            down_direct = os.getcwd() + '/data/edgar_master_index'

            filename = f'/master{year}QTR{qtr}.idx'
            path = ''.join([down_direct, filename])

            if not os.path.exists(path):
                print(url)
                with open(path, 'wb') as f:
                    f.write()

            qtr += 1
            time.sleep(7.5)

        except Exception:
            continue


def get_filings(ticker, form='10-K'):
    """scrapes master index files for enpoints
    these endpoints are used to download excel files of company financials
    provided by the SEC"""

    cik = get_cik_json(ticker)
    master_index = ''.join([os.getcwd(), '/data/edgar_master_index/'])

    directory = os.listdir(master_index)

    pattern = f'({form}).\d+.\d+.\d+.(edgar/data/{cik}/)(\d+.\d+.\d+)'

    downloads = []

    i = 0
    while i < 5:

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
        i += 1

    return downloads


def download_files(ticker, form='10-K'):
    """for downloading excel of company financials from SEC website"""

    filenames = get_filings(ticker, form=form)

    accession = [filename[-1] for filename in filenames]
    domain = [filename[-2] for filename in filenames]

    formatted_accession = [accession.pop().replace('-', '') for _ in
                           accession]

    formatted = [''.join([domain[0], number]) for number in formatted_accession]

    dstdir = ''.join([os.getcwd(), f'/data/{ticker}_reports/'])

    if not os.path.exists(dstdir):
        os.makedirs(dstdir)

    rename = f'{ticker}_report.xlsx'

    path = ''.join([dstdir, rename])

    for name in tqdm(formatted, bar_format='Downloading files'):

        i = 0
        while os.path.exists(path):
            rename = f'{ticker}_report_{i}.xlsx'
            path = ''.join([dstdir, rename])
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

    for file in dstdir:
        try:
            path = ''.join([path, file])


        except FileNotFoundError:
            continue

    return None


def column_change(df, inplace=True):

    """for changing column headers to the year of the statement and changes
    dataframes index"""

    pattern = r'\d{4}'
    dates = re.findall(pattern, str(df.loc[0]))

    headers = ['Accounts', ]
    for date in dates:
        headers.append(date)

    df.columns = headers

    try:
        df.set_index('Accounts', inplace=inplace)

    except Exception:
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


def percent_change(df):

    series = df.loc['Net sales']
    numeric = pd.to_numeric(series).rename('Growth rate')
    pct = pd.DataFrame(numeric).pct_change(periods=1).swapaxes('index',
                                                                'columns')
    pct_vals = pct.iloc[0].tolist()
    df.loc['Growth rate'] = pct_vals
    index = list(df.index)
    growth_rate = index.pop()
    index.insert(index.index('Net sales') + 1, growth_rate)
    df = df.reindex(index=index, columns=df.columns)

    return df


def union(dataframes):
    copies = [df.copy() for df in dataframes]
    cols = []
    for i in copies:
        for j in i.columns:
            cols.append(i.pop(j))

    df = pd.DataFrame()

    for i in cols:
        df[i.name] = i

    return df


def retrieve_income_statements(ticker):
    """imports downloaded excel files as pandas dataframes and appends them
    to list"""

    sheets = []

    path = ''.join([os.getcwd(), f'/data/{ticker}_reports/'])
    directory = os.listdir(path)

    for file in tqdm(directory):
        try:
            filename = ''.join([path, file])

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

def forecasted_income_statements(ticker):

    statements = retrieve_income_statements(ticker)
    [column_change(statement) for statement in statements]
    formatted = union(statements)
    formatted.set_index('Accounts', inplace=True)
    formatted.reindex(columns = sorted(list(formatted.columns)))
    formatted = percent_change(formatted)

    return formatted


def retrieve_balance_sheets(ticker):

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


def retrieve_cash_flow_statements(ticker):

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


def add_columns(df, periods=5):
    df = df.copy()

    year = int(df.columns[-1])
    first_period = year + 1
    last_period = first_period + periods

    new_cols = list(range(first_period, last_period))
    unpacked = [*list(df.columns), *new_cols]

    return unpacked


def excel_exception_helper(ticker):
    path = ''.join([os.getcwd(), f'/data/{ticker.lower()}_reports/'])
    directory = os.listdir(path)

    excel = []

    for workbook in directory:
        try:
            excel.append(pd.ExcelFile(path + workbook))

        except Exception:
            continue
    return excel


def statement_regex(ticker, statement='income'):
    excel = excel_exception_helper(ticker)

    if statement.lower() == 'income':
        r = re.compile('^.*.Inco*.$')

    elif statement.lower() == 'balance':
        r = re.compile('^.*.Balance*.$')

    elif statement.lower() == 'cash flow':
        r = re.compile('^.*.Cash*.$')

    for i in excel:
        sheet_name = list(filter(r.match, i.sheet_names)).pop()

        return sheet_name


def revenue_growth_rate(df):
    series = df.loc['Net sales']
    numeric = pd.to_numeric(series).rename('Growth rate')
    growth_rate = pd.DataFrame(numeric).pct_change(periods=1).swapaxes('index',
                                                                       'columns').iloc[
        0][-1]

    return growth_rate


def forecast_accounts(df, periods=5):
    growth_rate = revenue_growth_rate(df)
    factor = (1 + growth_rate)

    df = df.copy()

    new_df = pd.DataFrame(columns=add_columns(df))

    i = 1
    while i < len(df.index):
        row_name = df.iloc[i].name
        j = 1
        arrays = []
        array = df.iloc[i].to_numpy()

        while j < periods + 1:

            if j == 1:
                array = np.append(array, np.ceil(array[-1] * factor))
                arrays.append(array)

            else:
                array = np.append(array, np.ceil(arrays[-1][-1] * factor))
                arrays.append(array)

            j += 1

        series = pd.Series(arrays[-1]).rename(row_name)
        temporary_df = pd.DataFrame(series).swapaxes('index', 'columns')
        temporary_df.columns = add_columns(df)

        new_df.loc[row_name] = temporary_df.iloc[0]

        i += 1

    return new_df