"""
스타벅스 입점 전략 분석 (클러스터링, EDA, 시각화)
- 데이터 로딩, 클러스터링, 중심점 거리 계산, EDA, 시각화 등 함수화
- main 함수로 실행 구조화
- 경로/환경변수 상수화
- 반복/중복 코드 제거, 가독성 향상

Author: Jade
Date: 2025-07-06
"""

import os
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import geopandas as gpd
import contextily as ctx
from sklearn.cluster import KMeans, MeanShift
from sklearn.neighbors import BallTree

# 파일 경로 상수
DATA_PATH = 'ml_final_df.csv'
POP_PATH = 'df_population.csv'
SHAPE_PATH = 'LARD_ADM_SECT_SGG_11_202502.shp'


def load_data():
    df = pd.read_csv(DATA_PATH, encoding='utf-8-sig')
    df_population = pd.read_csv(POP_PATH, encoding='utf-8-sig')
    return df, df_population


def merge_population(df, df_population):
    df_population['가짜인덱스'] = df_population['31세_40세'] + df_population['계']
    df['가짜인덱스'] = df['31세_40세'] + df['계']
    df = df.merge(df_population[['시군구명', '읍면동명', '가짜인덱스']], how='left', on='가짜인덱스')
    return df


def elbow_method(X, max_k=13):
    wcss = []
    for k in range(2, max_k + 1):
        kmeans = KMeans(n_clusters=k, random_state=42)
        kmeans.fit(X)
        wcss.append(kmeans.inertia_)
    return wcss


def plot_elbow(wcss, k_star=7):
    range_k = list(range(2, 2 + len(wcss)))
    plt.figure(figsize=(8, 5))
    plt.plot(range_k, wcss, marker='o')
    plt.title("Elbow Method")
    plt.xlabel("Number of Clusters (K)")
    plt.ylabel("WCSS (Inertia)")
    plt.grid(True)
    idx_star = range_k.index(k_star)
    wcss_star = wcss[idx_star]
    plt.plot(k_star, wcss_star, marker='*', color='red', markersize=15, label="Optimal K")
    plt.legend()
    plt.show()


def run_meanshift(df, bandwidth=0.05, sample_size=1000):
    X = df[['경도', '위도']]
    X_idx = np.random.choice(len(X), size=min(sample_size, len(X)), replace=False)
    sample_X = X.iloc[X_idx]
    meanshift = MeanShift(bandwidth=bandwidth)
    meanshift.fit(sample_X)
    centers = meanshift.cluster_centers_
    return centers


def closest_radian(df1, df2, df2_col='index', lat_col='위도', lon_col='경도'):
    df1_coords = np.radians(df1[[lat_col, lon_col]].to_numpy())
    df2_coords = np.radians(df2[[lat_col, lon_col]].to_numpy())
    tree = BallTree(df2_coords, metric='haversine')
    distances, indices = tree.query(df1_coords, k=1)
    distances_km = distances.flatten() * 6371
    closest_values = df2.iloc[indices.flatten()][df2_col].reset_index(drop=True)
    return distances_km, closest_values


def plot_clusters(df, df_centers, shape_path=SHAPE_PATH):
    seoul_boundary = gpd.read_file(shape_path)
    seoul_boundary = seoul_boundary.to_crs(epsg=4326)
    fig, ax = plt.subplots(figsize=(10, 7))
    seoul_boundary.plot(ax=ax, edgecolor="black", facecolor="none")
    ctx.add_basemap(ax=ax, crs=seoul_boundary.crs, source=ctx.providers.OpenStreetMap.Mapnik)
    df.plot(ax=ax, x='경도', y='위도', linestyle="", color="grey", marker="o", markersize=0.5, label="상점")
    df_centers.plot(ax=ax, x='경도', y='위도', color='red', linestyle="", marker="X", markersize=10, label='상권중심점')
    plt.xlabel('경도')
    plt.ylabel('위도')
    plt.title('스타벅스와 위치')
    plt.legend()
    plt.show()


def add_age_ratios(df, age_cols):
    for col in age_cols:
        df[f'ratio_{col}'] = (df[col] / df['계']).round(2)
    return df


def main():
    # 데이터 로드 및 병합
    df, df_population = load_data()
    df = merge_population(df, df_population)

    # 클러스터링(Elbow)
    wcss = elbow_method(df[['경도', '위도']])
    plot_elbow(wcss, k_star=7)

    # MeanShift 중심점
    centers = run_meanshift(df)
    df_centers = pd.DataFrame(np.array(centers), columns=['경도', '위도'])
    df_centers['index'] = df_centers.index

    # 각 점에서 중심점까지 거리/클러스터 할당
    dist, cluster = closest_radian(df, df_centers, df2_col='index')
    df['중심점거리'] = dist
    df['중심점'] = cluster.values

    # 시각화
    plot_clusters(df, df_centers)

    # EDA: 스타벅스만 추출, 연령 비율 등
    df_starbucks = df[df['스타벅스여부'] == 1].copy()
    age_cols = ['0세_6세', '7세_19세', '20세_25세', '26세_30세', '31세_40세', '41세_50세', '51세_60세', '61세_109세']
    df_starbucks = add_age_ratios(df_starbucks, age_cols)

    # 예시: 박스플롯
    plt.figure(figsize=(16,8))
    if not df_starbucks.empty:
        sns.boxplot(data=df_starbucks, x='시군구명', y='중심점거리', color='green')
        mean_val = df_starbucks['중심점거리'].mean()
        plt.axhline(mean_val, color='red', linestyle='--', linewidth=2, label=f'평균: {mean_val:.1f}km')
        plt.legend()
        plt.ylim((0,3))
        plt.show()

if __name__ == "__main__":
    main()



