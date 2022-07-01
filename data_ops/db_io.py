import re
import pandas as pd
import requests
from bs4 import BeautifulSoup
import sqlalchemy as db
import os

# make session and user_agent
s = requests.Session()
user_agent = os.environ['USER_AGENT']

# engine creation and connection
db_engine = os.environ['ENGINE']
engine = db.create_engine(db_engine)

conn = engine.connect()
metadata = db.MetaData()

# table with data
table = os.environ['TABLE']

tickers = db.Table(table, metadata, autoload=True, autoload_with=engine)
query = db.select([tickers])


def add_column(engine, table_name, column):
    """function for adding an additional column to table"""
    column_name = column.compile(dialect=engine.dialect)
    column_type = column.type.compile(engine.dialect)
    engine.execute(
        "ALTER TABLE %s ADD COLUMN %s %s" % (
        table_name, column_name, column_type)
    )


def get_symbol_index(df, ticker):
    index = df.index[df["Symbol"] == ticker.upper()].tolist()
    index = index.pop()

    return index


def get_exchange(ticker):
    """for obtaining stock exchange of ticker"""
    # read in query
    df = pd.read_sql(query, conn)

    # get tickers index
    index = get_symbol_index(df, ticker)

    exchange = df.loc[index, ["Exchange"]]

    return exchange


def get_cik(ticker):
    # read in query
    df = pd.read_sql(query, conn)

    # get tickers index
    index = get_symbol_index(df, ticker)

    # determine if cik exists
    try:
        if len(df.loc[index, ["CIK"]]) <= 10:
            cik = df.loc[index, ["CIK"]]

    except ValueError:
        pass

    # if cik not in database
    # find cik on sec website
    url = s.get(
        f"https://sec.report/Ticker/{ticker.lower()}",
        headers={"User-Agent": user_agent}
    )

    soup = BeautifulSoup(url.text, "html.parser")
    scan = soup.find("h2")

    find = re.findall(r"(\d{10})", str(scan))

    cik = find.pop()

    # store cik in dataframe
    df.loc[index, ["CIK"]] = str(cik)

    # store dataframe back in sql
    df.to_sql("tickers", conn, if_exists="append", index=False)

    return cik

