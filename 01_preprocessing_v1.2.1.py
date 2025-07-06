"""
상권분석 데이터 전처리 모듈

이 모듈은 서울시 상권분석을 위한 다양한 데이터를 수집하고 전처리합니다.
- 스타벅스 매장 정보 크롤링
- 행정동 정보 및 좌표 변환
- 인구 데이터 전처리
- 지하철 승하차 데이터 처리
- 건물 실거래가 데이터 처리

Author: Data Analysis Team
Date: 2024
"""

import os
import time
import logging
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass

import pandas as pd
import numpy as np
import requests
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 환경변수 로드
load_dotenv()


@dataclass
class DataConfig:
    """데이터 설정 클래스"""
    API_KEY: str = os.getenv("VWORLD2_API_KEY", "")
    DATA_DIR: str = "./data"
    RAW_DATA_DIR: str = "./data/raw"
    OUTPUT_DIR: str = "./data/processed"
    
    # API 엔드포인트
    VWORLD_GEOCODE_URL: str = "https://api.vworld.kr/req/address"
    VWORLD_SEARCH_URL: str = "https://api.vworld.kr/req/search"
    
    # 스타벅스 크롤링 설정
    STARBUCKS_URL: str = "https://www.starbucks.co.kr/store/store_map.do"
    SEARCH_TIMEOUT: int = 10
    LOADING_DELAY: float = 2.0


class DataLoader:
    """데이터 로더 클래스"""
    
    def __init__(self, config: DataConfig):
        self.config = config
        self._ensure_directories()
    
    def _ensure_directories(self):
        """필요한 디렉토리 생성"""
        for directory in [self.config.DATA_DIR, self.config.RAW_DATA_DIR, self.config.OUTPUT_DIR]:
            os.makedirs(directory, exist_ok=True)
    
    def load_seoul_commercial_data(self) -> pd.DataFrame:
        """서울시 상가 정보 데이터 로드"""
        try:
            file_path = f"{self.config.RAW_DATA_DIR}/소상공인시장진흥공단_상가(상권)정보_서울_202503.csv"
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            logger.info(f"서울시 상가 데이터 로드 완료: {len(df)}개 레코드")
            return df
        except FileNotFoundError:
            logger.error(f"파일을 찾을 수 없습니다: {file_path}")
            raise
    
    def load_subway_data(self) -> pd.DataFrame:
        """지하철 데이터 로드"""
        try:
            file_path = f"{self.config.RAW_DATA_DIR}/서울교통공사_1_8호선 역별 일별 승객유형별 수송인원(환승유입인원 포함) 정보_20241231.CSV"
            df = pd.read_csv(file_path, encoding='euc-kr')
            logger.info(f"지하철 데이터 로드 완료: {len(df)}개 레코드")
            return df
        except FileNotFoundError:
            logger.error(f"파일을 찾을 수 없습니다: {file_path}")
            raise
    
    def load_population_data(self) -> pd.DataFrame:
        """인구 데이터 로드"""
        try:
            file_path = f"{self.config.RAW_DATA_DIR}/행정안전부_지역별(행정동) 성별 연령별 주민등록 인구수_20250531.csv"
            df = pd.read_csv(file_path, encoding='euc-kr')
            logger.info(f"인구 데이터 로드 완료: {len(df)}개 레코드")
            return df
        except FileNotFoundError:
            logger.error(f"파일을 찾을 수 없습니다: {file_path}")
            raise
    
    def load_building_price_data(self) -> pd.DataFrame:
        """건물 실거래가 데이터 로드"""
        try:
            file_path = f"{self.config.RAW_DATA_DIR}/상업업무용(매매)_실거래가_20250623165423.csv"
            df = pd.read_csv(file_path, encoding='euc-kr', skiprows=15)
            logger.info(f"건물 실거래가 데이터 로드 완료: {len(df)}개 레코드")
            return df
        except FileNotFoundError:
            logger.error(f"파일을 찾을 수 없습니다: {file_path}")
            raise


class StarbucksCrawler:
    """스타벅스 매장 정보 크롤러"""
    
    def __init__(self, config: DataConfig):
        self.config = config
        self.driver = None
    
    def _setup_driver(self):
        """웹드라이버 설정"""
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), 
            options=options
        )
    
    def _extract_store_info(self, store_element) -> Dict[str, str]:
        """매장 정보 추출"""
        try:
            name = store_element.get('data-name', '')
            lat = store_element.get('data-lat', '')
            lng = store_element.get('data-long', '')
            
            addr_element = store_element.select_one("p.result_details")
            addr = addr_element.get_text(" ", strip=True) if addr_element else ''
            
            return {
                "매장명": name,
                "위도": lat,
                "경도": lng,
                "주소": addr
            }
        except Exception as e:
            logger.warning(f"매장 정보 추출 실패: {e}")
            return {}
    
    def crawl_stores(self) -> pd.DataFrame:
        """스타벅스 매장 정보 크롤링"""
        try:
            self._setup_driver()
            wait = WebDriverWait(self.driver, self.config.SEARCH_TIMEOUT)
            
            # 페이지 접속
            self.driver.get(self.config.STARBUCKS_URL)
            
            # 지역검색 탭 클릭
            region_tab = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "loca_search")))
            region_tab.click()
            
            # 서울 선택
            seoul_xpath = '//a[contains(text(), "서울")]'
            seoul_btn = wait.until(EC.element_to_be_clickable((By.XPATH, seoul_xpath)))
            seoul_btn.click()
            
            # 전체 선택
            all_btn = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "set_gugun_cd_btn")))
            all_btn.click()
            
            # 로딩 대기
            time.sleep(self.config.LOADING_DELAY)
            
            # 매장 정보 수집
            soup = BeautifulSoup(self.driver.page_source, "lxml")
            stores = soup.select("li.quickResultLstCon")
            
            results = []
            for store in stores:
                store_info = self._extract_store_info(store)
                if store_info:
                    results.append(store_info)
            
            df = pd.DataFrame(results)
            logger.info(f"스타벅스 매장 정보 수집 완료: {len(df)}개 매장")
            
            return df
            
        except Exception as e:
            logger.error(f"스타벅스 크롤링 실패: {e}")
            raise
        finally:
            if self.driver:
                self.driver.quit()


class GeocodingService:
    """지오코딩 서비스 클래스"""
    
    def __init__(self, config: DataConfig):
        self.config = config
    
    def get_administrative_info(self, address: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        도로명주소로부터 행정동 정보 반환
        
        Args:
            address: 도로명주소
            
        Returns:
            Tuple[구명, 행정동명, 행정동코드]
        """
        params = {
            "service": "address",
            "request": "getcoord",
            "crs": "epsg:4326",
            "format": "json",
            "address": address,
            "type": "road",
            "key": self.config.API_KEY
        }
        
        try:
            response = requests.get(
                self.config.VWORLD_GEOCODE_URL, 
                params=params, 
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('response', {}).get('status') == 'OK':
                    structure = data['response']['refined']['structure']
                    return (
                        structure.get('level2'),  # 구명
                        structure.get('level4A'),  # 행정동명
                        structure.get('level4AC')  # 행정동코드
                    )
            
            logger.warning(f"API 응답 실패: {address}")
            return None, None, None
            
        except Exception as e:
            logger.error(f"지오코딩 API 호출 실패: {address}, {e}")
            return None, None, None
    
    def get_parcel_address(self, address: str) -> Optional[str]:
        """
        도로명주소로부터 지번주소 반환
        
        Args:
            address: 도로명주소
            
        Returns:
            지번주소
        """
        params = {
            "service": "search",
            "request": "search",
            "key": self.config.API_KEY,
            "query": address,
            "type": "address",
            "category": "road",
            "format": "json"
        }
        
        try:
            response = requests.get(
                self.config.VWORLD_SEARCH_URL, 
                params=params, 
                timeout=10
            )
            
            data = response.json()
            if data['response']['status'] == 'OK':
                return data['response']['result']['items'][0]['address']['parcel']
            
            logger.warning(f"지번주소 조회 실패: {address}")
            return None
            
        except Exception as e:
            logger.error(f"지번주소 API 호출 실패: {address}, {e}")
            return None
    
    def get_coordinates(self, address: str) -> Tuple[Optional[float], Optional[float]]:
        """
        주소로부터 좌표 반환
        
        Args:
            address: 주소
            
        Returns:
            Tuple[경도, 위도]
        """
        params = {
            "service": "address",
            "request": "getcoord",
            "crs": "epsg:4326",
            "address": address,
            "format": "json",
            "type": "PARCEL",
            "key": self.config.API_KEY
        }
        
        try:
            response = requests.get(
                self.config.VWORLD_GEOCODE_URL, 
                params=params, 
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('response', {}).get('status') == 'OK':
                    point = data['response']['result']['point']
                    return float(point['x']), float(point['y'])
            
            logger.warning(f"좌표 조회 실패: {address}")
            return None, None
            
        except Exception as e:
            logger.error(f"좌표 API 호출 실패: {address}, {e}")
            return None, None


class DataProcessor:
    """데이터 전처리 클래스"""
    
    def __init__(self, config: DataConfig):
        self.config = config
        self.geocoding_service = GeocodingService(config)
    
    def process_starbucks_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """스타벅스 데이터 전처리"""
        logger.info("스타벅스 데이터 전처리 시작")
        
        # 행정동 정보 추가
        logger.info("행정동 정보 추가 중...")
        admin_results = df['주소'].apply(
            lambda x: self.geocoding_service.get_administrative_info(x)
        )
        
        gu_names, dong_names, admin_codes = zip(*admin_results)
        df['행정동명'] = dong_names
        df['행정동코드'] = admin_codes
        
        # 누락된 행정동 정보 수동 보완
        missing_indices = df[df['행정동코드'].isna()].index
        if len(missing_indices) > 0:
            logger.info(f"누락된 행정동 정보 {len(missing_indices)}개 수동 보완")
            self._fill_missing_admin_info(df, missing_indices)
        
        # 지번주소 추가
        logger.info("지번주소 추가 중...")
        df['지번'] = df['주소'].apply(
            lambda x: self.geocoding_service.get_parcel_address(x)
        )
        
        # 누락된 지번 정보 수동 보완
        missing_jibun = df[df['지번'].isna()].index
        if len(missing_jibun) > 0:
            logger.info(f"누락된 지번 정보 {len(missing_jibun)}개 수동 보완")
            self._fill_missing_jibun_info(df, missing_jibun)
        
        # 지번주소 생성
        df['지번주소'] = "서울특별시 " + df['구'] + " " + df['지번']
        
        # 컬럼 정리
        df['스타벅스'] = 1
        df['상권업종대분류명'] = '음식'
        df['상권업종중분류명'] = '비알코올'
        df['상권업종소분류명'] = '카페'
        df['상가업소번호'] = 'sb' + df.index.astype(str)
        
        # 컬럼명 변경 및 정리
        df = df.rename(columns={
            '매장명': '상호명',
            '구': '시군구명',
            '행정동': '행정동명'
        })
        
        df = df.drop(columns=['주소', '지번'], axis=1)
        
        logger.info("스타벅스 데이터 전처리 완료")
        return df
    
    def _fill_missing_admin_info(self, df: pd.DataFrame, missing_indices):
        """누락된 행정동 정보 수동 보완"""
        manual_data = {
            259: ('대흥동', '1144010700'),
            382: ('문정1동', '1168010800'),
            426: ('목1동', '1147010100')
        }
        
        for idx, (dong_name, admin_code) in manual_data.items():
            if idx in df.index:
                df.loc[idx, '행정동명'] = dong_name
                df.loc[idx, '행정동코드'] = admin_code
    
    def _fill_missing_jibun_info(self, df: pd.DataFrame, missing_indices):
        """누락된 지번 정보 수동 보완"""
        manual_data = {
            259: "대흥동 111-1",
            426: "목동 920"
        }
        
        for idx, jibun in manual_data.items():
            if idx in df.index:
                df.loc[idx, '지번'] = jibun
    
    def process_population_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """인구 데이터 전처리"""
        logger.info("인구 데이터 전처리 시작")
        
        # 기본 정보 컬럼
        basic_cols = df.iloc[:, :6]
        
        # 성별 컬럼 분리
        male_cols = [col for col in df.columns if '남자' in col]
        female_cols = [col for col in df.columns if '여자' in col]
        
        df_male = df[male_cols]
        df_female = df[female_cols]
        
        # 컬럼명 정리
        df_male.columns = [col.replace('남자', '') for col in male_cols]
        df_female.columns = [col.replace('여자', '') for col in female_cols]
        
        # 전체 인구 계산
        df_total = df_male + df_female
        df_total = pd.concat([basic_cols, df_total], axis=1)
        
        # 연령대별 그룹화
        age_groups = [
            (0, 6, "어린이"),
            (7, 19, "학생"),
            (20, 25, "대학생"),
            (26, 30, "초년생"),
            (31, 40, "신혼부부"),
            (41, 50, "자녀부부"),
            (51, 60, "중년"),
            (61, 109, "노년")
        ]
        
        new_cols = []
        for start_age, end_age, group_name in age_groups:
            start_col = f'{start_age}세'
            end_col = f'{end_age}세'
            
            if start_col in df_total.columns and end_col in df_total.columns:
                start_loc = df_total.columns.get_loc(start_col)
                end_loc = df_total.columns.get_loc(end_col)
                
                new_col_name = f'{start_age}세_{end_age}세'
                df_total[new_col_name] = df_total.iloc[:, start_loc:end_loc+1].sum(axis=1)
                new_cols.append(new_col_name)
        
        # 결과 데이터프레임 생성
        result_df = pd.concat([basic_cols, df_total[new_cols]], axis=1)
        
        logger.info("인구 데이터 전처리 완료")
        return result_df
    
    def process_subway_data(self, df_1to8: pd.DataFrame, df_9: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """지하철 승하차 데이터 전처리"""
        logger.info("지하철 데이터 전처리 시작")
        
        # 1-8호선 데이터 처리
        df_1to8 = df_1to8.dropna(axis=0)
        df_1to8['이용객수'] = df_1to8.iloc[:, 7:].sum(axis=1)
        
        selected_cols = [
            '수송일자', '호선명', '역번호', '역명', '승하차구분', '이용객수',
            '07-08시간대', '08-09시간대', '09-10시간대',
            '17-18시간대', '18-19시간대', '19-20시간대'
        ]
        df_1to8 = df_1to8[selected_cols]
        
        # 9호선 데이터 처리
        numeric_cols = df_9.iloc[:, 12:30].columns
        for col in numeric_cols:
            df_9[col] = pd.to_numeric(df_9[col].str.replace(',', ''), errors='coerce')
        
        df_9['이용객수'] = df_9.iloc[:, 6:].sum(axis=1)
        
        # 9호선 컬럼명 맞추기
        df_9 = df_9[[
            '날짜', '호선', '역번호', '역사명', '구분', '이용객수',
            '07시-08시', '08시-09시', '09시-10시',
            '17시-18시', '18시-19시', '19시-20시'
        ]]
        
        # 구분값 정리
        df_9['구분'] = df_9['구분'].replace({
            '순승차': '승차',
            '순하차': '하차'
        })
        
        df_9.columns = df_1to8.columns
        
        # 데이터 통합
        df_combined = pd.concat([df_1to8, df_9], axis=0)
        df_combined = df_combined.round()
        
        # 날짜별 합계 후 역사별 평균
        group_cols = ['수송일자', '호선명', '역번호', '역명', '승하차구분']
        sum_cols = ['이용객수', '07-08시간대', '08-09시간대', '09-10시간대',
                   '17-18시간대', '18-19시간대', '19-20시간대']
        
        df_combined = df_combined.groupby(group_cols)[sum_cols].sum().reset_index()
        
        group_cols = ['호선명', '역번호', '역명', '승하차구분']
        df_combined = df_combined.groupby(group_cols)[sum_cols].mean().reset_index()
        
        # 시간대 합산
        df_combined['출근시간대'] = (df_combined['07-08시간대'] + 
                                   df_combined['08-09시간대'] + 
                                   df_combined['09-10시간대'])
        df_combined['퇴근시간대'] = (df_combined['17-18시간대'] + 
                                   df_combined['18-19시간대'] + 
                                   df_combined['19-20시간대'])
        
        # 승차/하차 데이터 분리
        df_on = df_combined[df_combined['승하차구분'] == '승차'].drop(columns='승하차구분')
        df_off = df_combined[df_combined['승하차구분'] == '하차'].drop(columns='승하차구분')
        
        # 컬럼명 변경
        df_on.columns = ['역명', '역번호', '호선명', '승차_이용객수', 
                        '승차_출근시간대', '승차_퇴근시간대']
        df_off.columns = ['역명', '역번호', '호선명', '하차_이용객수', 
                         '하차_출근시간대', '하차_퇴근시간대']
        
        logger.info("지하철 데이터 전처리 완료")
        return df_on, df_off
    
    def process_building_price_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """건물 실거래가 데이터 전처리"""
        logger.info("건물 실거래가 데이터 전처리 시작")
        
        # 필요한 컬럼만 선택
        selected_cols = ['시군구', '지번', '도로명', '용도지역', '건축물주용도', 
                        '전용/연면적(㎡)', '거래금액(만원)']
        df = df[selected_cols]
        
        # 주소 생성
        df['주소'] = df['시군구'] + ' ' + df['지번']
        
        # 좌표 추가
        logger.info("건물 좌표 정보 추가 중...")
        coords = df['주소'].apply(self.geocoding_service.get_coordinates)
        lons, lats = zip(*coords)
        
        df['경도'] = lons
        df['위도'] = lats
        
        # 결측값 제거
        df = df.dropna(axis=0)
        
        logger.info("건물 실거래가 데이터 전처리 완료")
        return df


class DataExporter:
    """데이터 내보내기 클래스"""
    
    def __init__(self, config: DataConfig):
        self.config = config
    
    def save_dataframe(self, df: pd.DataFrame, filename: str, encoding: str = 'utf-8-sig'):
        """데이터프레임을 CSV 파일로 저장"""
        file_path = f"{self.config.OUTPUT_DIR}/{filename}"
        df.to_csv(file_path, encoding=encoding, index=False)
        logger.info(f"데이터 저장 완료: {file_path}")


def main():
    """메인 실행 함수"""
    try:
        # 설정 초기화
        config = DataConfig()
        
        # 클래스 인스턴스 생성
        loader = DataLoader(config)
        crawler = StarbucksCrawler(config)
        processor = DataProcessor(config)
        exporter = DataExporter(config)
        
        # 1. 스타벅스 데이터 수집 및 전처리
        logger.info("=== 스타벅스 데이터 처리 시작 ===")
        starbucks_df = crawler.crawl_stores()
        starbucks_processed = processor.process_starbucks_data(starbucks_df)
        exporter.save_dataframe(starbucks_processed, "df_starbucks.csv")
        
        # 2. 서울시 상가 데이터 전처리
        logger.info("=== 서울시 상가 데이터 처리 시작 ===")
        seoul_df = loader.load_seoul_commercial_data()
        seoul_processed = seoul_df[['상가업소번호', '상호명', '상권업종대분류명', 
                                   '상권업종중분류명', '상권업종소분류명', '시군구명',
                                   '행정동코드', '행정동명', '지번주소', '도로명주소',
                                   '경도', '위도']].copy()
        seoul_processed['스타벅스'] = 0
        exporter.save_dataframe(seoul_processed, "df_seoul.csv")
        
        # 3. 인구 데이터 전처리
        logger.info("=== 인구 데이터 처리 시작 ===")
        population_df = loader.load_population_data()
        population_processed = processor.process_population_data(population_df)
        exporter.save_dataframe(population_processed, "df_population.csv")
        
        # 4. 지하철 데이터 전처리
        logger.info("=== 지하철 데이터 처리 시작 ===")
        subway_df = loader.load_subway_data()
        
        # 9호선 데이터는 별도 로드 필요
        df_subway_9 = loader.load_subway_9_data()
        subway_on, subway_off = processor.process_subway_data(subway_df, df_subway_9)
        exporter.save_dataframe(subway_on, "df_subway_on.csv")
        exporter.save_dataframe(subway_off, "df_subway_off.csv")
        
        # 5. 건물 실거래가 데이터 전처리
        logger.info("=== 건물 실거래가 데이터 처리 시작 ===")
        building_df = loader.load_building_price_data()
        building_processed = processor.process_building_price_data(building_df)
        exporter.save_dataframe(building_processed, "df_building_price.csv")
        
        logger.info("=== 모든 데이터 처리 완료 ===")
        
    except Exception as e:
        logger.error(f"데이터 처리 중 오류 발생: {e}")
        raise


if __name__ == "__main__":
    main()



