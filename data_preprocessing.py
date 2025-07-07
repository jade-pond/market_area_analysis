"""
---------수정중-------------
상권분석 데이터 전처리 모듈

이 모듈은 서울시 상권분석을 위한 다양한 데이터를 수집하고 전처리합니다.
- 스타벅스 매장 정보 크롤링
- 행정동 정보 및 좌표 변환
- 인구 데이터 전처리
- 지하철 승하차 데이터 처리
- 건물 실거래가 데이터 처리

Author: Jade
Date: 2025-07-06
"""
# %%
import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
from sklearn.neighbors import BallTree
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import time
import requests
import pandas as pd
import time
from dotenv import load_dotenv
import os

# %%
# 상권 데이터(전국)
# # https://www.data.go.kr/data/15083033/fileData.do
# https://www.data.go.kr/data/15099316/fileData.do?recommendDataYn=Y

# import pandas as pd
# import unicodedata
# import os

# folder = 'data'
# city_list = ['강원', '경기', '경남', '경북', '광주', '대구', '대전', '부산', '서울', '세종',
#              '울산', '인천', '전남', '전북', '제주', '충남', '충북']

# df_list = []

# for city in city_list:
#     city_norm = unicodedata.normalize('NFC', city.strip())  # 공백 제거 + 유니코드 정규화
#     filename = f'소상공인시장진흥공단_상가(상권)정보_{city_norm}_202503.csv'
#     path = os.path.join(folder, filename)

#     try:
#         df = pd.read_csv(path, encoding='utf-8-sig', low_memory=False)
#         df['지역'] = city_norm
#         df_list.append(df)
#         print(f"{city_norm} 불러옴: {df.shape}")
#     except Exception as e:
#         print(f"{city_norm} 실패: {e}")

# # 병합
# df_all = pd.concat(df_list, ignore_index=True)
# df_all.to_csv('all.csv', encoding='utf-8-sig', index=False)
# df = pd.read_csv('all.csv', encoding='utf-8-sig')
# df = df.set_index('상가업소번호')

# %%
# https://www.data.go.kr/data/15104835/fileData.do 환승인구
# https://www.data.go.kr/data/15062858/fileData.do 환승인구
# https://www.data.go.kr/data/15044250/fileData.do 서울교통공사_승하차순위
# https://www.data.go.kr/data/15099316/fileData.do?recommendDataYn=Y 지하철 위경도
# https://www.data.go.kr/data/15097972/fileData.do 인구수
# https://www.data.go.kr/data/15126468/openapi.do 아파트 실거래가 
# https://www.data.go.kr/data/15126463/openapi.do 
# https://rt.molit.go.kr/pt/xls/xls.do?mobileAt= 상업용 실거래가
# https://www.mois.go.kr/frt/bbs/type001/commonSelectBoardArticle.do?bbsId=BBSMSTR_000000000052&nttId=118603 행정기관코드

# https://velog.io/@kd01051/zerobase%EB%8D%B0%EC%9D%B4%ED%84%B0%EC%B7%A8%EC%97%85%EC%8A%A4%EC%BF%A8EDA%EA%B3%BC%EC%A0%9C1%EC%8A%A4%EB%B2%85%EC%9D%B4%EB%94%94%EC%95%BC-%EC%83%81%EA%B6%8C%EB%B6%84%EC%84%9D


# %%
mega_df = df[df['상호명'].str.contains('메가엠지씨커피', na=False)]
mega_df.to_csv('mega_df.csv', encoding='utf-8-sig')

# %%
df_seoul = pd.read_csv('./data/raw/소상공인시장진흥공단_상가(상권)정보_서울_202503.csv', encoding='utf-8-sig')
df_megamgc = pd.read_csv('mega_df.csv', encoding='utf-8-sig')
df_starbucks = pd.read_csv('starbucks_df.csv', encoding='utf-8-sig')

df_subway = pd.read_csv('subway.csv', encoding='utf-8-sig')
df_subway_usr = pd.read_csv('./data/raw/서울교통공사_1_8호선 역별 일별 승객유형별 수송인원(환승유입인원 포함) 정보_20241231.CSV', encoding='euc-kr')


df_admin_lat_lon = pd.read_excel('city_lat_lon.xlsx')
df_population = pd.read_csv('./data/raw/행정안전부_지역별(행정동) 성별 연령별 주민등록 인구수_20250531.csv', encoding='euc-kr')

df_building_price = pd.read_csv('./data/raw/상업업무용(매매)_실거래가_20250623165423.csv',
                                encoding='euc-kr',
                                skiprows=15)

# %% [markdown]
# # 1. df_starbucks

# %% [markdown]
# ### (1) 초기버전 - 매장찾기 > 퀵검색
# - 한계: 검색 결과 최대 30개 노출 이슈

# %%
# 스타벅스 크롤링 코드 v1.1
# store_url = "https://www.starbucks.co.kr/store/getStore.do"
# gugun_url = "https://www.starbucks.co.kr/store/getGugunList.do"

# sido_dict = {
#     "서울": "01", 
#     # "부산": "02", "대구": "03", "인천": "04", "광주": "05", "대전": "06", "울산": "07",
#     # "경기": "08", "강원": "09", "충북": "10", "충남": "11", "전북": "12", "전남": "13",
#     # "경북": "14", "경남": "15", "제주": "16", "세종": "17"
# }

# all_stores = []

# for sido_name, sido_code in sido_dict.items():
#     # 구군 목록 요청
#     gugun_res = requests.post(gugun_url, data={"sido_cd": sido_code})
#     guguns = gugun_res.json().get("list", [])

#     for gugun in guguns:
#         gugun_code = gugun["gugun_cd"]
#         payload = {
#             "ins_lat": "",
#             "ins_lng": "",
#             "p_sido_cd": sido_code,
#             "p_gugun_cd": gugun_code,
#             "in_biz_cd": "",
#             "set_date": ""
#         }

#         res = requests.post(store_url, data=payload)
#         stores = res.json().get("list", [])
#         all_stores.extend(stores)

#         print(f"{sido_name}-{gugun['gugun_nm']} 수집: {len(stores)}개")
#         time.sleep(0.2) 

# # DataFrame 정리
# df_all = pd.DataFrame(all_stores)
# df_all = df_all[["s_name", "tel", "doro_address", "lat", "lot", "sido_code", "gugun_code"]]
# df_all.columns = ["매장명", "전화번호", "주소", "위도", "경도", "시도코드", "구군코드"]

# print(f"✅ 전체 매장 수: {len(df_all)}개")


# %%
# post 방식으로 가져오면 서울 스타벅스 608개로 조회
# selenium으로 시 전체 조회 시 웹 상으론 645개 실제 데이터로는 675개 수집

# %% [markdown]
# ### (2) 수정버전 - 매장찾기 > 지역검색
# - selenium으로 클릭해 들어가서 bs4로 주소 클라스에 해당하는 값들 가져오기

# %%
# 스타벅스 크롤링 코드 v2.1

# 셀레니움 드라이버 설정
options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# 스타벅스 매장찾기 페이지 접속
driver.get("https://www.starbucks.co.kr/store/store_map.do")
wait = WebDriverWait(driver, 10)

# 지역검색 탭 클릭
region_tab = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "loca_search")))
region_tab.click()

# 서울 클릭 (XPath 사용)
seoul_xpath = '/html/body/div[4]/div[7]/div/form/fieldset/div/section/article[1]/article/article[2]/div[1]/div[2]/ul/li[1]/a'
seoul_btn = wait.until(EC.element_to_be_clickable((By.XPATH, seoul_xpath)))
seoul_btn.click()

# 전체 클릭
all_btn = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "set_gugun_cd_btn")))
all_btn.click()

# 로딩 대기
time.sleep(2)
soup = BeautifulSoup(driver.page_source, "lxml")

# 매장 정보 수집
stores = soup.select("li.quickResultLstCon")
results = []
for store in stores:
    name = store['data-name']
    lat = store['data-lat']
    lng = store['data-long']
    addr = store.select_one("p.result_details").get_text(" ", strip=True)
    results.append({"매장명": name, "위도": lat, "경도": lng, "주소": addr})

driver.quit()

df = pd.DataFrame(results)
# df.to_csv("스타벅스_서울_매장.csv", index=False)
# print(df.head())
df.info()

# %%
df.head()

# %% [markdown]
# ### (3) 행정동명, 지번주소 맵핑
# - 행정동명, 행정동코드
#     - 출처: 디지털 트윈국토 API (지오코더 API)
#     - 함수명: get_adm_from_doro
#     - 입력값(도로명주소) > 반환값(행정동)
#     - 작동: 로우별로 테이블의 도로명주소를 돌면서 API에 리퀘스트 보내고 행정동, 행정동코드 받음
#     - 칼럼명: 행정동, 행정동코드
#     - 조회 안 되는 값 3개에 대해선 mois.go.kr에서 행정기관코드 다운
# - 지번주소
#     - 출처: 디지털 트윈국토 API (search API)
#     - 함수명: get_jibun_from_doro
#     - 입력값(도로명주소) > 반환값(지번주소)
#     - 작동: 로우별로 테이블의 도로명주소를 돌면서 API에 리퀘스트 보내고 지번주소 받아냄
#     - 칼럼명: '지번', '지번주소'
#         - 지번: 동 + 번지
#         - 지번주소: 시 + 구 + 동 + 번지

# %%
# 스타벅스 행정동, 지번주소 변환하기 - 디지털트윈국토
# level1	문자	시·도
# level2	문자	시·군·구
# level3	문자	(일반구)구
# level4L	문자	(도로)도로명, (지번)법정읍·면·동 명
# level4LC	문자	(도로)도로코드, (지번)법정읍·면·동 코드
# level4A	문자	(도로)행정읍·면·동 명, (지번)지원안함
# level4AC	문자	(도로)행정읍·면·동 코드, (지번)지원안함
# level5	문자	(도로)길, (지번)번지

load_dotenv()
api_key = os.getenv("VWROLD2_API_KEY")

# 스타벅스의 시군구 읍면동 가져오기 - 국토 API
# input : 좌표 -> 행정동 반환

# def get_adm_from_coords(x, y, api_key):
#     """
#     좌표 입력 시 행정동 반환 함수
#     """
#     point_str = f"{x},{y}"

#     apiurl = "https://api.vworld.kr/req/address?"	
#     params = {	
#         "service": "address",	
#         "request": "getaddress",	
#         "crs": "epsg:4326",	
#         "point": point_str,	#리스트 내 스트링 형태로 받아야 함.
#         "format": "json",	
#         "type": "road",	#입력 주소 유형(ROAD, PARCEL) 
#         "key": api_key	
#     }	
#     data = requests.get(apiurl, params=params, timeout=5)	

#     if data.status_code == 200:	
#         # print(data.json()['response']['error']['text']) # 디버깅

#         try:
#             adm = data.json()['response']['result'][0]['structure']
#             # return adm
#             return adm['level2'], adm['level3'], adm['level4AC']  # 시구, 동, 행정동코드
#         except Exception as e:
#             print("리턴값 없음")

# 스타벅스의 시군구 읍면동 가져오기 - 국토 API
# input : 주소 -> 행정동 반환

# 행정동 반환 함수
def get_adm_from_doro(address, api_key): # geocoder API
    """
    도로명주소 입력 시 행정동 반환 함수
    """
    apiurl = "https://api.vworld.kr/req/address?"	
    params = {	
        "service": "address",	
        "request": "getcoord", # getcoord: 좌표를 받겠다
        "crs": "epsg:4326",	
        # "point": point_str,	#리스트 내 스트링 형태로 받아야 함.
        "format": "json",	
        "address": address,
        "type": "road",	#입력 주소 유형(ROAD, PARCEL)
        "key": api_key	
    }	
    try:
        data = requests.get(apiurl, params=params, timeout=10)
        if data.status_code == 200:	
            # print(data.json())
            # print(data.json()['response']['error']['text']) # 디버깅
            adm = data.json()['response']['refined']['structure']
            return adm['level2'], adm['level4A'], adm['level4AC']
        else: 
            print("status != 200")
        
    except Exception as e:
        return (None, None, None)

# https://api.vworld.kr/req/search?
# service=search&
# request=search&
# version=2.0&
# crs=EPSG:900913
# &bbox=14140071.146077,4494339.6527027,14160071.146077,4496339.6527027
# &size=10&page=1&query=성남시 분당구 판교로 242
# &type=address
# &category=road
# &format=json
# &errorformat=json
# &key=[KEY]


# 지번주소 반환 함수
def get_gibun_from_doro(address, api_key): # search API
    """
    도로명주소 입력 시 지번주소 반환 함수
    """
    apiurl = "https://api.vworld.kr/req/search?"
    params = {
        "service": "search", 
        "request": "search", #필수
        "key": api_key, #필수
        "query": address, #필수
        "type": "address", #필수
        "category": "road", #필수
        "format":"json" # 기본값임
    }

    data = requests.get(apiurl, params=params).json()
    
    if data['response']['status'] == 'OK':
        return data['response']['result']['items'][0]['address']['parcel']
    else:
        print(f"오류: {data['response']['status']}")


# %%
# # test
# test_adrs = df_starbucks.head(1)['주소'].values
# apiurl = "https://api.vworld.kr/req/search?"
# params = {
#     "service": "search", 
#     "request": "search", #필수
#     "key": api_key, #필수
#     "query": test_adrs, #필수
#     "type": "address", #필수
#     "category": "road", #필수
#     "format":"json" # 기본값임
# }

# data = requests.get(apiurl, params=params).json()
# result1 = data['response']['result']['items'][0]['address']['parcel']
# print(result1) #'역삼동 681'

# # 함수 테스트
# result2 = get_jibun_from_doro(test_adrs, api_key)
# print(result2) #'역삼동 681'

# %%
df_starbucks.head()

# %%
# # 1. 행정동 칼럼 생성
# # (1) regex로 주소 내 동 추출
# pattern = r'^([가-힣0-9\s\-]+?)(?=\(|,)'
# df_starbucks['행정동'] = df_starbucks['주소'].str.extract(pattern)

# # 행정동에 튜플로 불러와진 데이터 있어서 한번 더 수행
# df_starbucks['행정동'] = df_starbucks['행정동'].astype(str)
# pattern = r'([가-힣0-9]*동)'
# df_starbucks['행정동'] = df_starbucks['행정동'].str.extract(pattern)


# df_starbucks[df_starbucks['행정동'].isna()].shape # (107, 5)


# 1. 행정동, 행정동코드 칼럼 생성
load_dotenv()
api_key = os.getenv("VWORLD2_API_KEY")
query = "서울특별시 강남구 봉은사로 304"

result = df_starbucks['도로명주소'].apply(lambda x: get_adm_from_doro(x, api_key))
gu, dong, adm_cd = zip(*result)

df_starbucks['행정동명'] = dong # 3개 누락값있음
df_starbucks['행정동코드'] = adm_cd # 3개 누락값있음

# 네이버 지도 검색 후 수기 입력
idx = df_starbucks[df_starbucks['행정동코드'].isna()].index

df_starbucks.loc[259,'행정동명'] = '대흥동'
df_starbucks.loc[382,'행정동명'] = '문정1동'
df_starbucks.loc[426,'행정동명'] = '목1동'

adm = pd.read_excel('KIKcd_H.20250701.xlsx') # mois.go.kr에서 행정기관코드 다운
cond1 = adm ['시도명'] == '서울특별시'
cond2 = adm['읍면동명'].isin(['대흥동','문정1동','목1동'])
cond = (cond1) & (cond2)
cd_na_dict = dict(zip(adm[cond]['읍면동명'], adm[cond]['행정동코드']))

df_starbucks.loc[idx, '행정동코드'] = df_starbucks.loc[idx, '행정동명'].map(cd_na_dict)


# %%
# result['response']['refined']['structure']['level4A'],result['response']['refined']['structure']['level4AC']

# %%
df_starbucks[df_starbucks['행정동코드'].isna()]

# %%
# 2. 지번주소 칼럼 생성
# 함수 적용 - 2개 외 적용 완료
df_starbucks['지번'] = df_starbucks['주소'].apply(lambda x: get_gibun_from_doro(x, api_key))
# df_starbucks.info() # 지번 673 non-null object/ RangeIndex: 675 entries
# df_starbucks[df_starbucks['지번'].isna()] # index = [259,426], 매장명 = [서강대흥역, 목동SBS]

# 수기 입력
df_starbucks.loc[259,'지번'] = "대흥동 111-1"
df_starbucks.loc[426,'지번'] = "목동 920"

# 칼럼 생성: 지번주소
df_starbucks['지번주소'] = "서울특별시" + " " + df_starbucks['구']+ " " + df_starbucks['지번'] 


# %% [markdown]
# ### (4) df_seoul과 정합성을 위한 컬럼 생성

# %%
# df_seoul에서 사용할 컬럼 목록:
# '상가업소번호', '상호명', '상권업종대분류명', '상권업종중분류명', '상권업종소분류명', '시도명', '시군구명',
# '행정동코드', '행정동명', '법정동명', '지번주소', '도로명주소', '경도', '위도', 
df_starbucks['스타벅스'] = 1
df_starbucks['상권업종대분류명'] = '음식'
df_starbucks['상권업종중분류명'] = '비알코올'
df_starbucks['상권업종소분류명'] = '카페'
df_starbucks['상가업소번호'] = 'sb' + df_starbucks.index.astype(str)

df_starbucks = df_starbucks.drop(columns=['주소','지번'],axis=1)
df_starbucks = df_starbucks.rename(columns={'매장명':'상호명',
                                            '구':'시군구명',
                                            '행정동':'행정동명',
                                            })
len(df_starbucks.columns) # 12 행정동코드 외 완료

# %%
df_starbucks.to_csv('df_starbucks.csv', encoding='utf-8-sig', index=False)

# %% [markdown]
# # 2. df_seoul

# %% [markdown]
# 데이터 출처: https://www.data.go.kr/data/15083033/fileData.do

# %%
df_seoul = df_seoul[['상가업소번호', '상호명', 
    #  '지점명', '상권업종대분류코드', 
        '상권업종대분류명', 
    #  '상권업종중분류코드',
       '상권업종중분류명', 
    #    '상권업종소분류코드', 
       '상권업종소분류명', 
    #    '표준산업분류코드', '표준산업분류명', '시도코드',
      #  '시도명', 
    #    '시군구코드', 
       '시군구명', '행정동코드', '행정동명', 
    #    '법정동코드', 
      #  '법정동명', 
    #    '지번코드','대지구분코드', '대지구분명', '지번본번지', '지번부번지', 
       '지번주소',
    # '도로명코드', '도로명', '건물본번지','건물부번지', '건물관리번호', '건물명',
       '도로명주소', 
    #  '구우편번호', '신우편번호', '동정보', '층정보','호정보', 
       '경도', '위도']]
df_seoul['스타벅스'] = 0
df_seoul.to_csv('df_seoul.csv', encoding='utf-8-sig', index=False)

# %% [markdown]
# # 3. df_population

# %% [markdown]
# 데이터 출처: 
# - (1) 행정동 인구: https://www.data.go.kr/data/15097972/fileData.do
# - (2) 행정동 위경도: https://skyseven73.tistory.com/23, 누락 데이터는 수기 검색 후 보강.
# 
# - 행정동별 성별 연령별 주민등록 인구수 집계
# - 나이 구간 설정:
#     - ~6 어린이
#     -  7~19 학생
#     - 20~25 대학생
#     - 26~30 초년생
#     - 31~40 신혼부부 등
#     - 40~50 자녀를 둔 부부 등
#     - 50~60 중년
#     - 60~ 노년
# - 함수: make_bins
# 

# %% [markdown]
# ### (1) 나이 구간 설정

# %%
df_population = pd.read_csv('행정안전부_지역별(행정동) 성별 연령별 주민등록 인구수_20250531.csv', encoding='euc-kr')

# 0세부터 나이가 모두 나와있음. 정제 필요
import re
df_frame = df_population.iloc[:,:6]


col_male = [col for col in df_population.columns.to_list() if col in re.findall(r'.*남자.*', col)]
df_male = df_population[col_male]
col_male = [col.replace('남자','') for col in col_male]
df_male.columns = col_male

col_female = [col for col in df_population.columns.to_list() if col in re.findall(r'.*여자.*', col)]
df_female = df_population[col_female]
col_female = [col.replace('여자','') for col in col_female]
df_female.columns = col_female

df_total = df_male + df_female
df_total = pd.concat([df_frame, df_total], axis=1)

# 나이는 어떻게 나누면 좋을까?
## ~6 어린이
## 7~19 학생
## 20~25 대학생
## 26~30 초년생
## 31~40 신혼부부 등
## 40~50 자녀를 둔 부부 등
## 50~60 중년
## 60~ 노년

def make_bins(df):
    r = [(0,6), (7,19), (20,25), (26,30), (31,40), (41,50), (51,60), (61,109)]
    new_cols = []
    for start_age, end_age in r:

        start_col = f'{start_age}세'
        start_loc = df.columns.get_loc(start_col)

        end_col = f'{end_age}세'
        end_loc = df.columns.get_loc(end_col)
        new_col_name = f'{start_age}세_{end_age}세'
        df[new_col_name] = df.iloc[:, start_loc:end_loc+1].sum(axis=1)
        new_cols.append(new_col_name)
    return df, new_cols

df_total, pop_cols = make_bins(df_total)

df_city_population = pd.concat([df_frame, df_total[pop_cols]], axis=1)
df_city_population.head()
df_city_population.to_csv('df_city_population.csv', encoding='utf-8-sig', index=False)

# %% [markdown]
# ### (2) 위경도값 맵핑
# - 기본적으로 https://skyseven73.tistory.com/23 값 사용
# - 누락 데이터는 검색 후 수기 보강

# %%
# (1) df_population + city_lat_lon
# (2) df_population 
df_population = pd.read_csv('df_city_population.csv', encoding='utf-8-sig')
df_city_lat_lon = pd.read_excel('city_lat_lon.xlsx')


df_population = pd.merge(left=df_population,
                         right=df_city_lat_lon, 
                         how='left', 
                         left_on=['시군구명','읍면동명'], 
                         right_on=['시군구','읍면동/구'])
df_population = df_population.drop(columns=['시도','시군구', '읍/면/리/동', '리'], axis=1)


df_population = df_population[df_population['시도명'] == '서울특별시']
na_values = df_population[df_population['위도'].isna()]['읍면동명'].to_list() # ['회현동', '면목제3.8동', '상도제1동', '개포3동', '상일제1동', '상일제2동']


# 누락 데이터 수기 보강
na_dong_data = [
    {'읍면동명':'회현동', '위도':37.560547, '경도':126.979857},
    {'읍면동명':'면목제3.8동', '위도':37.589700, '경도':127.098500},
    {'읍면동명':'상도제1동', '위도':37.4989249, '경도':126.9508904},
    {'읍면동명':'개포3동', '위도':37.4984553, '경도':126.9145118},
    {'읍면동명':'상일제1동', '위도':37.55023, '경도':127.164711}, # Latitude : 37.55023 Longitude : 127.164711
    {'읍면동명':'상일제2동', '위도':37.5516582, '경도':127.1774908}, # 위도(Latitude) : 37.5516582 / 경도(Longitude) : 127.1774908
]
df_na_dong = pd.DataFrame(na_dong_data)

df_population = df_population.merge(df_na_dong, on='읍면동명', how='left', suffixes=('', '_보완'))

df_population['위도'] = df_population['위도'].fillna(df_population['위도_보완'])
df_population['경도'] = df_population['경도'].fillna(df_population['경도_보완'])

df_population.drop(columns=['위도_보완', '경도_보완'], inplace=True)

df_population.to_csv('df_population.csv', encoding='utf-8-sig', index=False)


# %%


# %%


# %% [markdown]
# # 4.df_building_price

# %% [markdown]
# ### (1) 컬럼 생성: 주소
# 데이처 출처: https://rt.molit.go.kr/pt/xls/xls.do?mobileAt=
# - 법정동 기준이라 행정동명과 차이가 있음
# - 칼럼 생성: 
#     - 주소: 시군구 + 지번
# ### (2) 컬럼 생성: 좌표
# - 컬람 생성:
#     - x: 경도
#     - y: 위도
#     - 함수: get_coords_vworld
#         - 지번주소 입력 시 좌표계 반환 
#     - 디지털트윈국토 API 활용

# %%
df_building_price = pd.read_csv('상업업무용(매매)_실거래가_20250623165423.csv',
                                encoding='euc-kr',
                                skiprows=15)
df_building_price.head(1).T

# %%
# (1) 칼럼 생성: 주소
df_building_price = df_building_price[['시군구','지번','도로명','용도지역','건축물주용도','전용/연면적(㎡)','거래금액(만원)']]
df_building_price.loc['주소'] = df_building_price['시군구'] + ' ' + df_building_price['지번']
df_building_price['도로명'].isna().sum() # 284


# %%
# (2) 칼럼 생성: 좌표
# 좌표 반환 함수
def get_coords_vworld(address):
    """지번주소 입력시 좌표 반환"""
    url = "https://api.vworld.kr/req/address"
    params = {
        "service": "address",
        "request": "getcoord",
        "crs": "epsg:4326",
        "address": address,
        "format": "json",
        "type": "PARCEL", #지번주소
        "key": "E6979757-8C90-364F-BBB7-AAAA254C4E08"
    }
    try:
        res = requests.get(url, params=params, timeout=0.5)
        if res.status_code != 200:
            return None, None
        data = res.json()
        if data.get('response', {}).get('status') == 'OK':
            data = res.json()['response']['result']['point']
            x = data['x']
            y = data['y']
            return x, y
    except Exception as e:
        print(f"오류 발생: {e} (주소: {address})")
    return None, None


lat_lon = df_building_price['주소'].apply(lambda x: get_coords_vworld(x))

lon, lat = zip(*lat_lon)
# lon, lat = lat_lon.apply(pd.Series)

df_building_price['경도'] = lon
df_building_price['위도'] = lat

df_building_price.head()
df_building_price = df_building_price.dropna(axis=0)
df_building_price.to_csv('df_building_price.csv', encoding='utf-8-sig')

# %%
# 함수 테스트 코드
# url = "https://api.vworld.kr/req/address"
# params = {
#     "service": "address",
#     "request": "getcoord",
#     "crs": "epsg:4326",
#     "address": "서울특별시 동작구 노량진동 283-3",
#     "format": "json",
#     "type": "PARCEL",
#     "key": "E6979757-8C90-364F-BBB7-AAAA254C4E08"}
# res = requests.get(url, params=params, timeout=1)

# result = res.json()
# result['response']['result']['point']['x'] #126.93601158189202
# result['response'].get('result') #{'crs': 'EPSG:4326', 'point': {'x': '126.93601158189202', 'y': '37.51084899878593'}}

# %%
df_building_price.to_csv('df_building.csv', encoding='utf-8-sig', index=False) 
df_building_price.info()

# %%


# %% [markdown]
# # 5.df_subway, df_subway_on , df_subway_off
# 
# - 참고: 횡단면 데이터로 2024-07-01 시점을 기준으로 하기 떄문에 시계열 반영 필요.
# - 데이터 출처: 
#     - https://www.data.go.kr/data/15099316/fileData.do?recommendDataYn=Y 역사 좌표 값
#     - https://www.data.go.kr/data/15104835/fileData.do 환승인구
#     - https://www.data.go.kr/data/15062858/fileData.do 환승인구
#     - https://www.data.go.kr/data/15044250/fileData.do 서울교통공사_승하차순위
# - df_subway: 지하철 역사 좌표값
# - df_subway_on, df_subway_off
#     - df_subway_usr1: 1호선 ~ 8호선 승하차 데이터
#     - df_subway_usr2: 9호선 승하차 데이터
#     - (1) 데이터 concat: df_subway_usr1, df_subway_usr2
#     - (2) 칼럼 생성: 출근 시간, 퇴근시간 승하차 수
#         - 출근 시간: 07시 ~ 10시 
#         - 퇴근 시간: 17시 ~ 20시
#     - (3) 데이터 프레임 분리: 승하차로 구분
# 

# %%
df_subway_usr1 = pd.read_csv('서울교통공사_1_8호선 역별 일별 시간대별 승객유형별 승하차인원_20241231.csv', encoding='euc-kr').dropna(axis=0)
df_subway_usr1['이용객수'] = df_subway_usr1.iloc[:,7:].sum(axis=1)
df_subway_usr1 = df_subway_usr1[['수송일자','호선명','역번호','역명','승하차구분',
                                #  '승객유형',
                                 '이용객수','07-08시간대','08-09시간대','09-10시간대','17-18시간대','18-19시간대','19-20시간대']]
df_subway_usr2 = pd.read_csv('서울교통공사_9호선2_3단계 역별일별시간대별승하차인원_20241031.csv', encoding='euc-kr')

# for col in df_subway_usr2.columns.to_list()[12:]:
#     df_subway_usr2[col] = df_subway_usr2[col].str.replace(',','')
#     df_subway_usr2[col] = df_subway_usr2[col].astype('int')

df_subway_usr2['이용객수'] = df_subway_usr2.iloc[:,6:].sum(axis=1)
df_subway_usr2 = df_subway_usr2[['날짜','호선','역번호','역사명','구분','이용객수','07시-08시','08시-09시','09시-10시','17시-18시','18시-19시','19시-20시']]

df_subway_usr2.loc[df_subway_usr2['구분'] == '순승차','구분'] = '승차'
df_subway_usr2.loc[df_subway_usr2['구분'] == '순하차','구분'] = '하차'

df_subway_usr2.columns = df_subway_usr1.columns

df_subway_usr = df_subway_usr.groupby(['수송일자','호선명','역번호','역명','승하차구분'])[['이용객수', '07-08시간대','08-09시간대','09-10시간대','17-18시간대','18-19시간대','19-20시간대']].sum().reset_index()
df_subway_usr = df_subway_usr.groupby(['호선명','역번호','역명','승하차구분'])[['이용객수', '07-08시간대','08-09시간대','09-10시간대','17-18시간대','18-19시간대','19-20시간대']].mean().reset_index()

# (1) 데이터 콘캣: 1호선-8호선 데이터 + 9호선 데이터
df_subway_usr = pd.concat([df_subway_usr1, df_subway_usr2], axis=0)
df_subway_usr = df_subway_usr.round()

# (2) 컬럼 생성: 출근시간대, 퇴근시간대
df_subway_usr['출근시간대'] = df_subway_usr['07-08시간대'] + df_subway_usr['08-09시간대'] + df_subway_usr['09-10시간대']
df_subway_usr['퇴근시간대'] = df_subway_usr['17-18시간대'] + df_subway_usr['18-19시간대'] + df_subway_usr['19-20시간대']
df_subway_usr = df_subway_usr[['역명','호선명','승하차구분','이용객수','출근시간대','퇴근시간대']]
# df_subway_usr.to_csv('df_subway_usr.csv', encoding='utf-8-sig')

# (3) df 분리: 승차, 하차
df_subway_on = df_subway_usr[df_subway_usr['승하차구분']=='승차'].drop(columns='승하차구분',axis=1)
df_subway_off = df_subway_usr[df_subway_usr['승하차구분']=='하차'].drop(columns='승하차구분',axis=1)
df_subway_on.columns = ['역명','호선명','승차_이용객수','승차_출근시간대','승차_퇴근시간대']
df_subway_off.columns = ['역명','호선명','하차_이용객수','하차_출근시간대','하차_퇴근시간대']

df_subway_on.to_csv('df_subway_on.csv', encoding='utf-8-sig', index=False)
df_subway_off.to_csv('df_subway_off.csv', encoding='utf-8-sig', index=False)

# %%
# (4) 데이터 01_preprocessing_for_ml에서 groupby로 보정 
df_subway_on[df_subway_on['역명'].str.contains(r'.*역$')]

# %%


# %% [markdown]
# # 6. df_subway

# %%
df_subway = pd.read_csv('subway.csv', encoding='utf-8-sig')
df_subway = df_subway.iloc[:,1:]
df_subway.head()

# %%



