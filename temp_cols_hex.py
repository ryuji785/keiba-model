# -*- coding: utf-8 -*-
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
import re
path=Path('data/raw/jra/race_202508040812.html')
html=path.read_text(encoding='utf-8')
soup=BeautifulSoup(html,'html.parser')
th_place=soup.find('th', class_='place')
table=th_place.find_parent('table') if th_place else None
if table is None:
    for t in soup.find_all('table'):
        if t.find(string=re.compile('着順')):
            table=t
            break
if table:
    df=pd.read_html(str(table))[0]
    for col in df.columns:
        print(col, [hex(ord(ch)) for ch in str(col)])
else:
    print('no table')
