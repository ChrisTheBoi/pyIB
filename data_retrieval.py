from sec_edgar_api import EdgarClient
from bs4 import BeautifulSoup
import urllib.request as url_r
from user_agent import generate_user_agent
import re
from sec_edgar_api import EdgarClient

random_ua = generate_user_agent()
edgar = EdgarClient(user_agent=random_ua)

def get_cik(ticker):
    """function to retrieve CIK of company"""
    url = url_r.Request(f'https://sec.report/Ticker/{ticker.lower()}',
                        headers={'User-Agent': f'{random_ua}'})
    url = url_r.urlopen(url)

    soup = BeautifulSoup(url, 'html.parser')
    scan = soup.find('h2')

    cik = re.findall(r'(\d{10})', str(scan))
    cik = cik.pop()

    return cik

def get_submissions_(ticker):
    cik_num = get_cik(ticker)
    request = edgar.get_submissions(cik=cik_num)
    return request

def get_company_concept_(ticker,tag,taxonomy='us-gaap'):
    cik_num = get_cik(ticker)
    request = edgar.get_company_concept(cik=cik_num,taxonomy=taxonomy,tag=tag)
    return request
