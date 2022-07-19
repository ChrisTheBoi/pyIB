import datetime as dt
import os
import numpy as np
import pandas as pd
import pandas_datareader as pdr
from dateutil.relativedelta import relativedelta
from fredapi import Fred
from data_ops import DataSQL


class IncomeStatement(DataSQL):
    def __init__(self, ticker):
        super().__init__(ticker)
        self.income_statements = self.load_income_statements()


    def union(self, statements):
        copies = [df.copy() for df in statements]
        cols = []
        for i in copies:
            for j in i.columns:
                cols.append(i.pop(j))

        df = pd.DataFrame()

        for i in cols:
            if i.name not in df.columns:
                df[i.name] = i

        return df


    def formatted_income_statement(self):

        formatted = self.union(self.income_statements)

        try:
            formatted.set_index('Accounts', inplace=True)

        except KeyError:
            pass

        formatted = formatted.reindex(columns=sorted(formatted.columns)).fillna(0)

        return formatted


    def add_columns(self, periods=5):
        df = self.formatted_income_statement().copy()

        year = int(df.columns[-1])
        first_period = year + 1
        last_period = first_period + periods

        new_cols = list(range(first_period, last_period))
        unpacked = [*list(df.columns), *new_cols]

        return unpacked


    def revenue_growth_rate(self):
        rev_accts = ['Net sales', 'Revenue']
        for i in rev_accts:
            try:
                series = self.formatted_income_statement().loc[i]
            except Exception:
                continue

        numeric = pd.to_numeric(series).rename('Growth rate')
        growth_rate = \
        pd.DataFrame(numeric).pct_change(periods=1).swapaxes('index',
                                                             'columns').iloc[0][
            -1]

        return growth_rate


    def forecast_accounts(self, df, periods=5):
        growth_rate = self.revenue_growth_rate()
        factor = (1 + growth_rate)

        df = df.copy()

        new_df = pd.DataFrame(columns=self.add_columns())

        i = 1
        while i < len(df.index):
            row_name = df.iloc[i].name
            j = 1
            arrays = []
            array = df.iloc[i].to_numpy(dtype=np.float64)

            while j < periods + 1:

                if j == 1:
                    array = np.append(array,
                                      np.ceil(np.nanprod(array[-1]) * factor))
                    arrays.append(array)

                else:
                    array = np.append(array, np.ceil(
                        np.nanprod(arrays[-1][-1]) * factor))
                    arrays.append(array)

                j += 1

            series = pd.Series(arrays[-1]).rename(row_name)
            temporary_df = pd.DataFrame(series).swapaxes('index', 'columns')
            temporary_df.columns = self.add_columns()

            new_df.loc[row_name] = temporary_df.iloc[0]

            i += 1

        return new_df


    def forecasted_income_statement(self):
        return self.forecast_accounts(self.formatted_income_statement())


class Risk(DataSQL):

    def __init__(self, ticker, api_key=os.environ['API_KEY']):
        super().__init__(ticker)
        self.exchange = self.get_exchange_json()
        self.fred = Fred(api_key=api_key)


    def get_inflation_rate(self, base_year='1983-08-01'):
        cpi = self.fred.get_series('CPIAUCSL')
        df = pd.DataFrame(cpi)

        rate = (df.iloc[-1] - df.loc[base_year]) / df.loc[base_year]

        return rate


    def get_risk_free_rate(self):
        tbills = self.fred.get_series('TB3MS')
        inflation_rate = self.get_inflation_rate()

        df = pd.DataFrame(tbills)

        rate = 1 + df.iloc[-1] / 1 + inflation_rate

        return rate

    def get_market_rate(self, start=5, end=dt.datetime.now(), interval='m'):
        start = dt.datetime.now() - relativedelta(years=start)

        data = pdr.get_data_yahoo(self.exchange, start, end, interval=interval)
        data = data['Adj Close']

        log_returns = np.log(data / data.shift())

        array = pd.DataFrame(log_returns).to_numpy()

        rate = np.nanmean(array)

        return rate


class CAPM(Risk):
    """class for CAPM of tickers"""

    def __init__(self, ticker):
        super().__init__(ticker)

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


    def capm(self):
        beta = self.beta()
        market_rate = self.get_market_rate()
        risk_free_rate = self.get_risk_free_rate()

        risk_premium = market_rate - risk_free_rate

        capm = risk_premium + (beta * risk_premium)

        return capm
