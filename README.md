# 스타벅스(서울) 상권분석 프로젝트

## 프로젝트 개요
- **목적**: 서울시 내 스타벅스와 메가커피의 입점 전략 및 상권 특성 분석
- **주요 데이터**: 상가정보, 인구, 지하철, 실거래가 등

## 주요 파일 설명
- 00_data_load_v3-1.ipynb:
   - (ETL) 스타벅스 데이터 크롤링, 디지털 트윈 국토 API 연동, 데이터 유효성 검증 등.
- distance_utils.py:
   - 데이터 전처리 모듈. 위경도 기반 거리 변환 및 가까운 거리 추출 (haversine, BallTree 기반).
     - BallTree: 원 기반 트리구조. 데이터 탐색 효율화를 위해 사용. 예컨대 근방 카페수를 찾을 때 전체 조회하는 대신 자기 구역 내 데이터만 조회하고 나머지는 버리는 형태로 계산 효율성 증가. 
- 01_preprocessing_v3-1.ipynb:
   - 데이터 결합을 위한 전처리. 데이터별 예외적 특성 다량 발생으로 모듈화 실패.
- 02_analysis_store_clustering_v3-1.ipynb
   - 상권 클러스터링: KMeans, DBSCAN, MeanShift
   - 클러스터링 평가: Davies-Bouldin, Calinski-Harabasz, 실루엣 계수(성능 이슈로 샘플 기반 진행) 
- 03_starbucks_eda_v3-2.ipynb: 스타벅스 데이터 기반 EDA 및 기술적 통계 분석.
- 03_mgc_eda_v3-2.ipynb: 메가mgc 데이터 기반 EDA 및 기술적 통계 분석.
- 04_analysis_comparison_v3-2.ipynb: 스타벅스 및 메가커피 비교 분석. (업로드 예정. ~ 30.AUG.2025)



## 실행 방법
1. **필수 패키지 설치**
   ```bash
   distance_utils.py
   ```
2. **분석 및 시각화 실행** -- (분석 및 시각화 모듈 생성 예정. 30.AUG.2025)

   ```bash
   ```
   - 결과 차트: (준비중)
   - 상세 보고서: (준비중)

## 결과물 안내
(준비중)

## 환경
- Python 3.10+

---

> 문의 및 개선 제안은 이슈로 남겨주세요! 
