from user_agent import generate_user_agent
from data_ops import DataJSON
import datetime as dt
import os
import re
import pandas as pd
import requests
from ratelimit import limits
from tqdm.auto import tqdm
import constants as c


random_ua = generate_user_agent()

heads = {'Host': 'www.sec.gov', 'Connection': 'close',
         'Accept': 'application/json, text/javascript, */*; q=0.01', 'X-Requested-With': 'XMLHttpRequest',
         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36',
         }

ticker = 'amkr'

downloads = c.get_filings()

filenames = c.get_filings('amkr', form='10-K')

accession = [filename[-1] for filename in filenames]
domain = [filename[-2] for filename in filenames]

formatted_accession = [accession.pop().replace('-', '') for _ in
                       accession]

formatted = [''.join([domain[0], number]) for number in formatted_accession]

for name in tqdm(formatted, bar_format='Downloading files'):

    i = 0
    while os.path.exists(path):
        rename = f'{ticker}_report_{i}.xlsx'
        path = ''.join([dstdir, rename])
        i += 1

        url = f'https://www.sec.gov/Archives/{name}/Financial_Report.xlsx'

        req = requests.get(url, headers=heads, stream=True)

        if req.status_code == 200:
            with open(path, 'wb') as f:
                for chunk in req.iter_content(chunk_size=15000):
                    f.write(chunk)

        else:
            continue
