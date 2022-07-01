import os
import numpy as np
import pandas as pd
import pandas_datareader as pdr
import datetime as dt
from dateutil.relativedelta import relativedelta
from data_ops.retrieval import DataRetrieval, get_exchange
from fredapi import Fred

key = os.environ['API_KEY']

fred = Fred(api_key=f'{key}')


def get_inflation_rate(base_year='1983-08-01'):

    cpi = fred.get_series('CPIAUCSL')
    df = pd.DataFrame(cpi)

    rate = (df.iloc[-1] - df.loc[base_year]) / df.loc[base_year]

    return rate


def get_risk_free_rate():

    tbills = fred.get_series('TB3MS')
    inflation_rate = get_inflation_rate()

    df = pd.DataFrame(tbills)

    rate = 1 + df.iloc[-1] / 1 + inflation_rate

    return rate


class CAPM(DataRetrieval):

    """class for CAPM of tickers"""

    def __init__(self, ticker):
        super().__init__(ticker)
        self.exchange = get_exchange(self.ticker)


    def beta(self, start=5, end=dt.datetime.now(), interval='m'):

        tickers = [self.ticker, self.exchange]

        start = dt.datetime.now() - relativedelta(years=start)

        data = pdr.get_data_yahoo(tickers, start, end, interval=interval)
        data = data['Adj Close']

        log_returns = np.log(data / data.shift())

        # get covariance and variance
        cov = log_returns.cov()
        var = log_returns[self.exchange].var()

        # get beta
        beta = cov.loc[self.ticker, self.exchange] / var

        return beta


    def get_market_rate(self, start=5, end=dt.datetime.now(), interval='m'):

        start = dt.datetime.now() - relativedelta(years=start)

        data = pdr.get_data_yahoo(self.exchange, start, end, interval=interval)
        data = data['Adj Close']

        log_returns = np.log(data / data.shift())

        array = pd.DataFrame(log_returns).to_numpy()

        rate = np.nanmean(array)

        return rate


    def capm(self):

        ticker = CAPM(self.ticker)

        beta = ticker.beta()
        market_rate = ticker.get_market_rate()
        risk_free_rate = get_risk_free_rate()

        risk_premium = market_rate - risk_free_rate

        capm = risk_premium + (beta * risk_premium)

        return capm


def capms(*tickers):
    capms = []

    for ticker in tickers:
        capm = CAPM(ticker).capm()
        capms.append(capm)

    return capms


