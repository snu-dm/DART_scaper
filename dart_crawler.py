import numpy as np
import pandas as pd
import requests
from urllib.request import urlopen
import urllib.parse
from zipfile import ZipFile
from io import BytesIO
import xml.etree.ElementTree as et
import datetime as dt
import calendar
from bs4 import BeautifulSoup
from tqdm import tqdm
import time
import json

MARKET_CODE_DICT = {
    'kospi': 'stockMkt',
    'kosdaq': 'kosdaqMkt',
    'konex': 'konexMkt'
}

DOWNLOAD_URL = 'kind.krx.co.kr/corpgeneral/corpList.do'

api_key = 'a7b222155e0ad5b1ed9de6838174eb585a7db8c9'
request_url = 'https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key='

# download stock codes and company names from DART
def download_stock_codes(market=None, delisted=False):
    params = {'method': 'download'}

    if market.lower() in MARKET_CODE_DICT:
        params['marketType'] = MARKET_CODE_DICT[market]

    if not delisted:
        params['searchType'] = 13

    params_string = urllib.parse.urlencode(params)
    download_url = urllib.parse.urlunsplit(['http', DOWNLOAD_URL, '', params_string, ''])
    print(f"download_url: {download_url}")

    df = pd.read_html(download_url, header=0, encoding='euc-kr')[0]
    df.종목코드 = df.종목코드.map('{:06d}'.format)

    return df

# set search date for query
def setSearchDate(year, quarter):
    if quarter == 4:
        end_year = str(year+1)
        end_month = '03'
        end_day = str(calendar.monthrange(year+1,3)[1])

    else:
        end_year = str(year)
        end_month = ('0'+str(5+(quarter-1)*3))[-2:]
        end_day = str(calendar.monthrange(year+1,int(end_month))[1])

    end_date = dt.datetime.strptime(end_year+end_month+end_day,'%Y%m%d').date()
    search_window = dt.timedelta(days=90)
    start_date = (end_date-search_window).strftime('%Y%m%d')

    return start_date, end_date.strftime('%Y%m%d')

# 불러올 data structure: {key: 종목명_종목코드, value: 보고서에 대한 dictionary} 
# 보고서에 대한 dictionary: {key: 보고서 type, value: dataframe}

def constructDictionary(): # 불러올 data를 담을 container
    storage = {}  # 종목명_종목코드를 key로 사용
    for index, row in tqdm(corp_info.iterrows()):
        storage[row.종목명 + '_' + row.종목코드] = {}
    return storage

def recordINFO(soup_body): # 
    global doc_dict

    #보고서명
    report_nm = soup_body.find('report_nm').text
    #접수번호
    rcept_no = soup_body.find('rcept_no').text
    #접수일자
    rcept_dt = soup_body.find('rcept_dt').text

    data = pd.DataFrame()
    data.at[0,'보고서명'] = report_nm
    data.at[0,'접수번호'] = rcept_no
    data.at[0,'접수일자'] = rcept_dt

    return data

# get docuument info by url(parameter 수집은 위의 함수들에서)
def getDocumentInfo(corp, doctype, year, quarter, page_no=1, page_count=100):
    global doc_dict, rogue_corps

    #고유번호 look-up
    stock_code = corp.split('_')[1]
    corp_code = df.at[df['종목코드'].eq(stock_code).idxmax(),'고유번호']
    #검색기간 설정
    start_date, end_date = setSearchDate(year,quarter)

    url = 'https://opendart.fss.or.kr/api/list.xml?crtfc_key='+api_key+'&corp_code='+corp_code+'&bgn_de='+start_date+'&end_de='+end_date+'&pblntf_detail_ty='+doctype+'&page_no='+str(page_no)+'&page_count='+str(page_count)

    r = requests.get(url)
    soup = BeautifulSoup(r.text, features='lxml')

    if soup.find('status').text=='000':
        infos = recordINFO(soup)
        doc_dict[corp][doctype] = infos.to_dict('records')

    else:
        time.sleep(np.random.randint(1,1500)/500)
        url2 = 'https://opendart.fss.or.kr/api/list.xml?crtfc_key='+api_key+'&corp_code='+corp_code+'&bgn_de='+start_date+'&end_de='+end_date+'&pblntf_ty=A&page_no='+str(page_no)+'&page_count='+str(page_count)

        r2 = requests.get(url2)
        soup2 = BeautifulSoup(r2.text, features='lxml')

        if soup2.find('status').text=='013':
            rogue_corps.append(corp)
            pass
        else:
            infos = recordINFO(soup2)
            doc_dict[corp][doctype] = infos.to_dict('records')

    return

kospi_stocks = download_stock_codes('kospi')
kospi_stocks['시장구분'] = 'KOSPI'
kospi_stocks = kospi_stocks[['회사명','종목코드']]

kosdaq_stocks = download_stock_codes('kosdaq')
kosdaq_stocks['시장구분'] = 'KOSDAQ'
kosdaq_stocks = kosdaq_stocks[['회사명','종목코드']]

corp_info = pd.concat([kospi_stocks, kosdaq_stocks]).reset_index(drop=True)
corp_info = corp_info.rename(columns={'회사명':'종목명'})
corp_info.to_csv('corporation_information_2020.csv', index=False)

r = urlopen(request_url+api_key)

with ZipFile(BytesIO(r.read())) as zf:
    file_list = zf.namelist()
    while len(file_list) > 0:
        file_name = file_list.pop()
        corpCode = zf.open(file_name).read().decode()

tree = et.fromstring(corpCode)
stocklist = tree.findall('list')

corp_codes = [item.findtext("corp_code") for item in stocklist]
corp_names = [item.findtext("corp_name") for item in stocklist]
stock_codes = [item.findtext("stock_code") for item in stocklist]
modify_dates = [item.findtext("modify_date") for item in stocklist]

wanted_stocks = corp_info['종목코드'].tolist()

corp_codes = [corp_codes[i] for i in range(len(corp_codes)) if stock_codes[i] in wanted_stocks]
corp_names = [corp_names[i] for i in range(len(corp_names)) if stock_codes[i] in wanted_stocks]
modify_dates = [modify_dates[i] for i in range(len(modify_dates)) if stock_codes[i] in wanted_stocks]
stock_codes = [stock_codes[i] for i in range(len(stock_codes)) if stock_codes[i] in wanted_stocks]

df = pd.DataFrame()
df['종목코드'] = stock_codes
df['종목명'] = corp_names
df['고유번호'] = corp_codes
df['최근변경일자'] = modify_dates

df.to_csv('DART_corpCodesXstockCodes_Xwalk.csv', encoding='utf8', index=False)

doc_dict = constructDictionary()
corp_keys = list(doc_dict.keys())
corp_keys.sort()

tp = 'A002'
year = 2019
quarter = 3
page_no = 1
page_count = 10
rogue_corps = []

#for key in tqdm(corp_keys):
#    getDocumentInfo(key, tp, year, quarter, page_no, page_count)
#    time.sleep(np.random.randint(1,2000)/500)

for i in tqdm(range(4)):
    getDocumentInfo(corp_keys[i], tp, year, quarter, page_no, page_count)
    time.sleep(np.random.randint(1,2000)/500)
    print("********")

print(f"doc_dict: {doc_dict[corp_keys[0]]}")