import os
from data_ops import db_io
from sec_edgar_api import EdgarClient
import requests
import json


user_agent = os.environ.get('USER_AGENT')
edgar = EdgarClient(user_agent=user_agent)
s = requests.Session()
storage = "symbol_info.json"


def get_cik(ticker):

    cik = db_io.get_cik(ticker)

    return cik


def get_exchange(ticker):

    try:
        exchange = db_io.get_exchange(ticker)

    except IndexError:
        with open(storage, "r+") as f:
            symbols = json.load(f)

        if ticker in symbols["AMEX"]:
            exchange = "^XAX"

        elif ticker in symbols["Nasdaq"]:
            exchange = "^IXIC"

        elif ticker in symbols["NYSE"]:
            exchange = "^NYA"

        else:
            cik = get_cik(ticker)
            subs = edgar.get_submissions(cik)

            for key, val in subs.items():
                if key == "exchanges":
                    name = val.pop()

                    if name == "Nasdaq":
                        exchange = "^IXIC"

                    elif name == "NYSE":
                        exchange = "^NYA"

                    elif name == "AMEX":
                        exchange = "^XAX"

        with open(storage, "r+") as f:
            json.dump(symbols, f)
            f.close()

    return exchange


class DataRetrieval:
    """class for accessing SEC API"""

    def __init__(self, ticker):
        self.ticker = ticker
        self.cik = get_cik(ticker)

    def get_submissions_(self):
        request = edgar.get_submissions(cik=self.cik)
        return request

    def get_company_concept_(self, tag, taxonomy="us-gaap"):
        request = edgar.get_company_concept(cik=self.cik, taxonomy=taxonomy, tag=tag)
        return request

    def get_company_facts_(self):
        request = edgar.get_company_facts(cik=self.cik)
        return request

    def get_exchange(self):

        try:
            exchange = db_io.get_exchange(self.ticker)

        except IndexError:
            with open(storage, "r+") as f:
                symbols = json.load(f)

            if self.ticker in symbols["AMEX"]:
                exchange = "^XAX"

            elif self.ticker in symbols["Nasdaq"]:
                exchange = "^IXIC"

            elif self.ticker in symbols["NYSE"]:
                exchange = "^NYA"

            else:
                cik = get_cik(self.ticker)
                subs = edgar.get_submissions(cik)

                for key, val in subs.items():
                    if key == "exchanges":
                        name = val.pop()

                        if name == "Nasdaq":
                            exchange = "^IXIC"

                        elif name == "NYSE":
                            exchange = "^NYA"

                        elif name == "AMEX":
                            exchange = "^XAX"

            with open(storage, "r+") as f:
                json.dump(symbols, f)
                f.close()

            return exchange


    def get_account(self, account="AccountsPayableCurrent", form="10-K"):
        company_facts = edgar.get_company_facts(cik=self.cik)

        accounts = {}
        valz = []
        years = []

        for key, values in company_facts["facts"].items():
            for key, values in values.items():
                if key == account:

                    for key, valuez in values["units"].items():
                        for values in valuez:

                            if values["form"] == form:
                                for keys, vals in values.items():

                                    if keys == "val":
                                        if values.get("val") not in valz:
                                            valz.append(values.get("val"))

                                    if keys == "end":
                                        if values.get("end") not in years:
                                            years.append(values.get("end"))

                                    accounts["date"] = years

                                    accounts[f"{account}"] = valz

                                    return accounts

