import datetime as dt
import json
import os
from pathlib import Path
import re
import pandas as pd
import requests
from ratelimit import limits
from tqdm.auto import tqdm
import sqlalchemy as db
from sqlalchemy_utils import database_exists, create_database


class DataJSON:

    def __init__(self, ticker):
        self.filepath = Path(''.join([os.getcwd(), '/data/company_tickers.json']))
        self.ticker = ticker


    def load_json(self):
        """for loading data from the default filepath, but, if necessary,
        also loads other json files"""

        try:
            data = pd.read_json(self.filepath)

        except ValueError:
            with open(self.filepath) as f:
                data = json.load(f)

        return data


    def save_json(self, df):
        """for saving loaded data back to default filepath
        does not support storing any objects other than pd.DataFrame"""

        try:
            df.to_json(path_or_buf=self.filepath)

        except ValueError:
            pass


    def get_cik_json(self):
        """loads company's CIK from default filepath"""

        with open(self.filepath) as f:
            data = json.load(f)

        for info in data.values():
            if info['ticker'] == self.ticker.upper():
                cik = info['cik_str']

                return cik


    def get_exchange_json(self):
        """loads company's exchange from default filepath"""

        with open(self.filepath) as f:
            f = json.load(f)

            for key, value in f.items():
                if value['ticker'] == self.ticker.upper():
                    exchange = value['exchange']

                    return exchange


class DataSEC(DataJSON):

    def __init__(self, ticker):
        super().__init__(ticker)
        self.cik = self.get_cik_json()

        self.heads = {'Host': 'www.sec.gov', 'Connection': 'close',
                      'Accept': 'application/json, text/javascript, */*; q=0.01',
                      'X-Requested-With': 'XMLHttpRequest',
                      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36',
                      }


    @limits(calls=10, period=1)
    def download_master_index(self, year=dt.date.today().year):

        qtr = 1
        while qtr < 5: 
            try:
                url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
                response = requests.get(url, headers=self.heads)
                response.raise_for_status()

                down_direct = Path(''.join([os.getcwd(),'/data/edgar_master_index']))

                filename = f'/master{year}QTR{qtr}.txt'
                path = Path(''.join([down_direct, filename]))

                if not os.path.exists(path):
                    print(url)
                    with open(path, 'wb') as f:
                        f.write(response.content)

                qtr += 1

            except requests.HTTPError:
                continue


    def get_year(self, df):
        r = re.compile(r'\d{4}')
        dates = r.findall(str(df.loc[0]))

        return dates

    @limits(calls=10, period=1)
    def to_csv(self, url, statement=None, form='10-K'):
        req = requests.get(url, headers=self.heads, stream=True)

        if statement == 'income':
            sheet_names = [
                'Consolidated Statements of Inco',
                'CONSOLIDATED STATEMENTS OF INCO',
                'Consolidated_Statements_of_Inco',
                'Consolidated Statements of Oper',
                'CONSOLIDATED STATEMENTS OF OPER'
            ]
            folder = 'income_statements'

        elif statement == 'balance':
            sheet_names = [
                'Consolidated_Balance_Sheets',
                'Consolidated Balance Sheets',
                'CONSOLIDATED BALANCE SHEETS'
            ]
            folder = 'balance_sheets'

        elif statement == 'cash':
            sheet_names = [
                'Consolidated_Statements_of_Cash',
                'Consolidated Statements of Cash',
                'CONSOLIDATED STATEMENTS OF CASH'
            ]
            folder = 'cash_flow_statements'

        try:
            cwd = os.getcwd()
            dst = f'/data/{self.ticker.lower()}_reports/{form}s/csv/{folder}/'

            directory = Path(''.join([cwd, dst]))
            if not os.path.exists(directory):
                os.makedirs(directory)

            temporary_xlsx = f'{self.ticker.lower()}.xlsx'
            temp_path = Path(''.join([cwd, '/', temporary_xlsx]))
            if req.status_code == 200:
                with open(temp_path, 'wb') as f:
                    for chunk in req.iter_content(chunk_size=15000):
                        f.write(chunk)

            df = pd.read_excel(temporary_xlsx)
            year_ended = self.get_year(df)[0]

            filename = f'{self.ticker.lower()}_{year_ended}.csv'
            path = Path(''.join([cwd, dst, filename]))

            if not os.path.exists(path):


                pbar = tqdm(sheet_names)
                for name in pbar:
                    pbar.set_description(name)
                    try:

                        df = pd.read_excel(temp_path, sheet_name=name)
                        df.to_csv(path, index=False)

                    except ValueError:
                        continue

            os.remove(temp_path)

        except UnboundLocalError:
            pass

        if statement == None:
            statements = ['income', 'balance', 'cash']
            [self.to_csv(url, statement=stmt) for stmt in statements]


    @limits(calls=10, period=1)
    def get_filings(self, form='10-K'):
        """scrapes master index files for enpoints
        these endpoints are used to download excel files of company financials
        provided by the SEC"""

        master_index = Path(''.join([os.getcwd(), '/data/edgar_master_index/']))

        directory = os.listdir(master_index)

        r = re.compile(f'({form}).(\d+.\d+.\d+).(edgar/data/{self.cik}/)(' \
                       f'\d+.\d+.\d+)')

        downloads = []

        i = 0
        pbar = tqdm(total=len(directory))
        pbar.set_description('Scanning master index')
        while i < len(directory):

            for file in directory:
                doc = master_index + file

                with open(doc, encoding='utf-8') as f:
                    try:
                        regex = r.findall(f.read())

                        for item in regex:
                            downloads.append(item)

                    except UnicodeDecodeError:
                        continue

            pbar.update(1)
            i += 1

        return downloads


    @limits(calls=10, period=1)
    def download_files(self, statement=None, form='10-K'):
        """for downloading excel of company financials from SEC website"""
        downloads = self.get_filings()

        accession = [download[-1] for download in downloads]
        domain = [download[-2] for download in downloads]

        formatted_accession = [accession.pop().replace('-', '') for _ in
                               accession]

        formatted = [''.join([domain[0], number]) for number in
                     formatted_accession]

        i = 0
        pbar = tqdm(total=len(formatted))

        while i < len(formatted):
            pbar.set_description(f'Downloading {formatted[i]}')
            try:
                url = f'https://www.sec.gov/Archives/' \
                      f'{formatted[i]}/Financial_Report.xlsx'

                self.to_csv(url, statement=statement, form=form)

            except Exception:
                continue

            pbar.update(1)
            i += 1
        pbar.close()


    def column_change(self, statements):
        """for changing column headers to the year of the statement and changes
        dataframes index"""

        for df in statements:
            try:
                pattern = r'\d{4}'
                dates = re.findall(pattern, str(df.loc[0]))

                headers = ['Accounts', ]
                for date in dates:
                    headers.append(date)

                df.columns = headers

                try:
                    index = list(df['Accounts']).index(
                        'Income Statement [Abstract]')
                    df.drop(index, inplace=True)

                except IndexError:
                    continue

            except Exception:
                continue


    def load_income_statements(self, form='10-K'):

        sheets = []

        path = Path(''.join([os.getcwd(), f'/data/{self.ticker}_reports/'
                                     f'{form}s/csv/income_statements/']))

        if not os.path.exists(path):
            self.download_files(statement='income', form=form)

        directory = os.listdir(path)

        pbar = tqdm(directory)
        for file in pbar:
            pbar.set_description(f'Loading income statements from {file}')
            try:
                filename = ''.join([path, file])
                df = pd.read_csv(filename)
                sheets.append(df)

            except Exception:
                continue

        self.column_change(sheets)

        return sheets


    def load_balance_sheets(self, form='10-K'):

        sheets = []

        path = Path(''.join([os.getcwd(), f'/data/{self.ticker.lower()}_reports/'
                                     f'{form}s/csv/balance_sheets/']))

        if not os.path.exists(path):
            self.download_files(statement='balance', form=form)

        directory = os.listdir(path)

        pbar = tqdm(directory)
        for file in pbar:
            pbar.set_description(f'Loading balance sheets from {file}')
            try:
                filename = ''.join([path, file])
                df = pd.read_csv(filename)
                sheets.append(df)

            except Exception:
                continue

        self.column_change(sheets)

        return sheets


    def load_cash_flow_statements(self, form='10-K'):

        sheets = []

        path = Path(''.join([os.getcwd(), f'/data/{self.ticker.lower()}_reports/'
                                     f'{form}s/csv/cash_flow_statements/']))

        if not os.path.exists(path):
            self.download_files(statement='cash', form=form)

        directory = os.listdir(path)

        if len(directory) == 0:
            self.to_csv(statement='cash')

        pbar = tqdm(directory)
        for file in pbar:
            pbar.set_description(f'Loading income statements from {file}')
            try:
                filename = ''.join([path, file])
                df = pd.read_csv(filename)
                sheets.append(df)

            except Exception:
                continue

        self.column_change(sheets)

        return sheets


class DataSQL(DataSEC):
    def __init__(self, ticker):
        super().__init__(ticker)
        url = os.environ['DB_URL']

        self.engine = db.create_engine(''.join([url, self.ticker.lower()]))

        if not database_exists(self.engine.url):
            create_database(self.engine.url)

        self.conn = self.engine.connect()

    def csv_to_sql(self, statement=None, form='10-K'):

        try:
            if statement == 'income':
                statements = self.load_income_statements_csv()
                folder = 'income_statements'

            elif statement == 'balance':
                statements = self.load_balance_sheets_csv()
                folder = 'balance_sheets'

            elif statement == 'cash':
                statements = self.load_cash_flow_statements_csv()
                folder = 'cash_flow_statements'

            try:
                path = Path(''.join(
                    [os.getcwd(), f'/data/{self.ticker.lower()}_reports/'
                                  f'{form}s/csv/{folder}/']))

                if not os.path.exists(path):
                    os.makedirs(path)

                i = 0
                while i < len(statements):
                    statements[i].to_sql(f'{statement}_{i}', self.conn,
                                         if_exists='replace')
                    i += 1

            except Exception:

                statements = ['income', 'balance', 'cash']
                for i in statements:
                    self.csv_to_sql(statement=i, form=form)

        except Exception:
            self.to_csv()
            self.csv_to_sql()


    '''def create_table(self, table_name):
        metadata = db.Metadata()
        table = db.Table(table_name, metadata, autoload=True,
                         autoload_with=self.engine)
        query = db.select([table])'''
