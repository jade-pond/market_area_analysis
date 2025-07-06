# 서울시 스타벅스 상권분석 프로젝트

## 사용 데이터 ERD
![image](https://github.com/user-attachments/assets/c052b9f2-fdc6-4817-a419-01891f51545e)


## 프로젝트 개요
- **목적**: 서울시 내 스타벅스와 메가커피의 입점 전략 및 상권 특성 분석
- **주요 데이터**: 상가정보, 인구, 지하철, 실거래가 등

## 주요 파일 설명
- `data_preprocessing.py` : 데이터 수집, 전처리, 크롤링, 가공
- `starbucks_clustering_analysis.py` : 스타벅스 클러스터링 및 EDA
- `starbucks_eda_visualization.py` : 스타벅스 데이터 탐색 및 시각화
- `starbucks_megacoffee_comparison.py` : 스타벅스 vs 메가커피 비교 분석
- `quick_analysis.py` : 빠른 요약 분석 및 시각화 (한글 폰트 지원)
- `analysis_report.md` : 분석 결과 요약 보고서

## 실행 방법
1. **필수 패키지 설치**
   ```bash
   pip install -r requirements.txt
   pip install koreanize-matplotlib
   ```
2. **샘플 데이터 생성**
   ```bash
   python create_sample_data.py
   ```
3. **분석 및 시각화 실행**
   ```bash
   python quick_analysis.py
   ```
   - 결과 차트: `analysis_results.png`
   - 상세 보고서: `analysis_report.md`

## 결과물 안내
- `analysis_results.png` : 브랜드별 분포, 거리, 평당거래금액 등 주요 비교 시각화
- `analysis_report.md` : 분석 요약 및 인사이트

## 환경
- Python 3.10+
- 주요 패키지: pandas, numpy, matplotlib, seaborn, scikit-learn, koreanize-matplotlib 등

---

> 문의 및 개선 제안은 이슈로 남겨주세요! 
