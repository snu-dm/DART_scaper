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
import re

MARKET_CODE_DICT = {
    'kospi': 'stockMkt',
    'kosdaq': 'kosdaqMkt',
    'konex': 'konexMkt'
}

DOWNLOAD_URL = 'kind.krx.co.kr/corpgeneral/corpList.do'

api_key = 'a7b222155e0ad5b1ed9de6838174eb585a7db8c9'
request_url = 'https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key='

current_path = '/home/5yoondori/bdai_dart/DART_scraper/'

# DART에서 stock codes 및 company names 다운로드
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

# 쿼리 검색을 위해 년도, 분기 입력시 해당 분기  시작/말일 출력하는 함수
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

# 불러올 data structure: {key: 종목명_종목코드, value: 보고서에 대한 dict} 
# 보고서에 대한 dict: {key: 보고서 type, value: dataframe}

def constructDictionary(): # 불러올 보고서들의 data를 담을 container
    storage = {}  # 종목명_종목코드를 key로 사용
    for index, row in tqdm(corp_info.iterrows()):
        storage[row.종목명 + '_' + row.종목코드] = {}
    return storage

def recordINFO(soup_body): # 보고서 문건의 html을 담은 bs객체에서 필요한 메타 데이터들을 df로 출력하는 함수
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

def getDocumentInfo(corp, doctype, year, quarter, page_no=1, page_count=100): # url로 보고서 문서 정보를 가져오는 함수(parameter 수집은 위의 함수들에서)
    global doc_dict, rogue_corps

    #고유번호 look-up
    stock_code = corp.split('_')[1]
    corp_code = df.at[df['종목코드'].eq(stock_code).idxmax(),'고유번호']
    #검색기간 설정
    start_date, end_date = setSearchDate(year,quarter)

    url = 'https://opendart.fss.or.kr/api/list.xml?crtfc_key='+api_key+'&corp_code='+corp_code+'&bgn_de='+start_date+'&end_de='+end_date+'&pblntf_detail_ty='+doctype+'&page_no='+str(page_no)+'&page_count='+str(page_count)

    r = requests.get(url)
    soup = BeautifulSoup(r.text, features='xml')

    if soup.find('status').text=='000':
        infos = recordINFO(soup)
        doc_dict[corp][doctype] = infos.to_dict('records')

    else:
        time.sleep(np.random.randint(1,1500)/500)
        url2 = 'https://opendart.fss.or.kr/api/list.xml?crtfc_key='+api_key+'&corp_code='+corp_code+'&bgn_de='+start_date+'&end_de='+end_date+'&pblntf_ty=A&page_no='+str(page_no)+'&page_count='+str(page_count)

        r2 = requests.get(url2)
        soup2 = BeautifulSoup(r2.text, features='xml')

        if soup2.find('status').text=='013':
            rogue_corps.append(corp)
            pass
        else:
            infos = recordINFO(soup2)
            doc_dict[corp][doctype] = infos.to_dict('records')

    return

def latestDisclosures(tp="A001"): # 모든 보고서 목록이 저장된 doc_dict에서, 가장 최근의 1개의 공시보고서 정보를 얻는 함수
    global rogue_corps
    latest_disclosures = pd.DataFrame(columns=['종목코드', '기업명','보고서명','접수번호','접수일자'])
    index = 0
    rogue_corps = []

    for key in tqdm(corp_keys):
        try:
            종목코드 = '_'+key.split('_')[1]
            기업명 = key.split('_')[0]
            보고서명 = doc_dict[key][tp][0]['보고서명']
            접수번호 = doc_dict[key][tp][0]['접수번호']
            접수일자 = doc_dict[key][tp][0]['접수일자']

            latest_disclosures.loc[index] = [종목코드, 기업명, 보고서명, 접수번호, 접수일자]
            index += 1
        except:
            rogue_corps.append(key)

    return latest_disclosures

def getDocumentNumber(disclosure_df): # 기존에 가지고 있던 보고서 문서에 대한 정보들을 이용해, 보고서의 문서번호 열을 추가해주는 함수
    base = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo='
    dcm_no = []
    for index, row in tqdm(disclosure_df.iterrows()):

        url = base + row.접수번호
        r = requests.post(url)
        time.sleep(np.random.randint(1,500)/100)
        soup = BeautifulSoup(r.text,'lxml')
        time.sleep(np.random.randint(1,800)/100)
        dn = 0

        script_tag = soup.find('script', string=lambda s: s and 'node1' in s)
        match = re.search(r"node1\['dcmNo'\] = \"(\d+)\";", script_tag.text)
        if match:
          dn = match.group(1)
        else:
          print("문서번호 정보를 찾을 수 없습니다.")
        dcm_no.append(dn)

    disclosure_df['문서번호'] = dcm_no
    return

def getDownloadLink(disclosure_df): # 기존에 가지고 있던 보고서 문서에 대한 정보들을 이용해, 보고서의 문서번호 열을 추가해주는 함수
    base_pdf = 'http://dart.fss.or.kr/pdf/download/pdf.do?'
    base_xls = 'http://dart.fss.or.kr/pdf/download/excel.do?'

    pdf = []
    excel = []
    for index, row in tqdm(disclosure_df.iterrows()):
        rcp_no = row.접수번호
        dcm_no = row.문서번호

        pdf_link = base_pdf + 'rcp_no=' + rcp_no + '&dcm_no=' + dcm_no
        xls_link = base_xls + 'rcp_no=' + rcp_no + '&dcm_no=' + dcm_no

        pdf.append(pdf_link)
        excel.append(xls_link)

    disclosure_df['보고서링크'] = pdf
    disclosure_df['재무제표링크'] = excel
    return
# 함수 선언 끝
# 메인 코드 시작

# 코스피, 코스닥 데이터 다운로드
kospi_stocks = download_stock_codes('kospi')
kospi_stocks['시장구분'] = 'KOSPI'
kospi_stocks = kospi_stocks[['회사명','종목코드']]

kosdaq_stocks = download_stock_codes('kosdaq')
kosdaq_stocks['시장구분'] = 'KOSDAQ'
kosdaq_stocks = kosdaq_stocks[['회사명','종목코드']]

# 코스피, 코스닥 데이터 concat 하여 하나의 df로 만들기
corp_info = pd.concat([kospi_stocks, kosdaq_stocks]).reset_index(drop=True)
corp_info = corp_info.rename(columns={'회사명':'종목명'})
# corp_info.to_csv('corporation_information_2020.csv', index=False)

# dart에서 가능한 기업들 목록 다 가져오기
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

# 우리가 가진 코스피, 코스닥 기업들에 대한 자료들만 추려내자
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

#df.to_csv('DART_corpCodesXstockCodes_Xwalk.csv', encoding='utf8', index=False)

# 기업 목록 정리 끝
# 보고서 데이터 저장 시작
doc_dict = constructDictionary()
corp_keys = list(doc_dict.keys())
corp_keys.sort()

# 검색하고자 하는 내용에 따라 매개변수 변경
tp = 'A001'
year = 2023
quarter = 3
page_no = 1
page_count = 10
rogue_corps = []
'''
for key in tqdm(corp_keys):
    getDocumentInfo(key, tp, year, quarter, page_no, page_count)
    time.sleep(np.random.randint(1,2000)/500)
'''

# 이미 준비된 변수들로 실제 공시 정보 가져오기
for i in tqdm(range(10)): # request초과를 방지하기 위해 10개의 기업들에 대해서만 시행
    getDocumentInfo(corp_keys[i], tp, year, quarter, page_no, page_count)
    time.sleep(np.random.randint(1,2000)/500)
    print(f"doc_dict: {doc_dict[corp_keys[i]]}")

# 최신 공시 정보만 추출하기
latest_disclosures = latestDisclosures('A001')

print(f"최신 공시보고서 추출에 실패한 횟수: {len(rogue_corps)}") # 10개에 대해서만 보고서를 추출했으니, 코스피 코스닥 전체 종목 수 - 10 이 출력될것. 20240122 기준 전체 종목수 2538

getDocumentNumber(latest_disclosures)
getDownloadLink(latest_disclosures)
latest_disclosures.to_csv(current_path + 'latest_disclosures_240122.csv',index=False, encoding='utf8')