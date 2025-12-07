from scripts.jra_past_links import get_srl_cnames
from scripts.etl_common import http_post, decode_shift_jis
from bs4 import BeautifulSoup
import re

srls = get_srl_cnames(2024,5)
print('srl count', len(srls))
if srls:
    srl = srls[0]
    print('first', srl)
    resp = http_post('https://www.jra.go.jp/JRADB/accessS.html', data={'cname': srl})
    html = decode_shift_jis(resp.content)
    print('html len', len(html))
    soup = BeautifulSoup(html, 'html.parser')
    links = []
    for a in soup.find_all('a', href=True):
        if 'pw01sde' in a['href']:
            links.append(a['href'])
    onclick_pat = re.compile(r"pw01sde[^'\" >]+")
    for tag in soup.find_all(onclick=True):
        for m in onclick_pat.finditer(tag.get('onclick','')):
            links.append(m.group(0))
    print('links', links[:10])
