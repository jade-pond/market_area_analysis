"""
스타벅스 vs 메가커피 입점 전략 비교 분석 (클러스터링, EDA, 시각화)
- 데이터 로딩, 클러스터링, 중심점 거리 계산, EDA, 시각화 등 함수화
"""

import os
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import geopandas as gpd
import contextily as ctx
from sklearn.cluster import KMeans
from sklearn.neighbors import BallTree

# 파일 경로 상수
DATA_PATH = 'ml_final_df.csv'
SHAPE_PATH = 'LARD_ADM_SECT_SGG_11_202502.shp'


def load_data():
    df = pd.read_csv(DATA_PATH, encoding='utf-8-sig')
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


def run_kmeans(df, n_clusters=10):
    kmeans = KMeans(n_clusters=n_clusters, init="k-means++", random_state=42)
    kmeans.fit(df[['경도', '위도']])
    centers = kmeans.cluster_centers_
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
    plt.title('스타벅스/메가커피와 위치')
    plt.legend()
    plt.show()


def add_age_ratios(df, age_cols):
    for col in age_cols:
        df[f'ratio_{col}'] = (df[col] / df['계']).round(2)
    return df


def main():
    # 데이터 로드
    df = load_data()

    # 클러스터링(Elbow)
    wcss = elbow_method(df[['경도', '위도']])
    plot_elbow(wcss, k_star=7)

    # KMeans 중심점
    centers = run_kmeans(df, n_clusters=10)
    df_centers = pd.DataFrame(np.array(centers), columns=['경도', '위도'])
    df_centers['index'] = df_centers.index

    # 각 점에서 중심점까지 거리/클러스터 할당
    dist, cluster = closest_radian(df, df_centers, df2_col='index')
    df['중심점거리'] = dist
    df['중심점'] = cluster.values

    # 시각화
    plot_clusters(df, df_centers)

    # EDA: 스타벅스/메가커피 비교
    cond_mega = df['상호명'].str.contains('메가커피') | df['상호명'].str.contains('메가엠지씨')
    df_megamgc = df[cond_mega].copy()
    df_starbucks = df[df['스타벅스여부'] == 1].copy()

    # 예시: 근접역거리, 중심점거리, 평당거래금액, 출근시간대 등 비교 히스토그램
    plt.figure(figsize=(8, 5))
    plt.hist(df_megamgc['근접역거리'], bins=30, alpha=0.6, label='메가커피', color='orange', density=True)
    plt.hist(df_starbucks['근접역거리'], bins=30, alpha=0.6, label='스타벅스', color='green', density=True)
    plt.xlabel('근접역거리')
    plt.ylabel('빈도수')
    plt.title('스타벅스 VS 메가커피')
    plt.legend()
    plt.grid(True)
    plt.show()

    plt.figure(figsize=(8, 5))
    plt.hist(df_megamgc['중심점거리'], bins=30, alpha=0.5, label='메가커피', color='orange', density=True)
    plt.hist(df_starbucks['중심점거리'], bins=30, alpha=0.6, label='스타벅스', color='green', density=True)
    plt.xlabel('중심점거리')
    plt.ylabel('빈도수')
    plt.title('스타벅스 VS 메가커피')
    plt.legend()
    plt.grid(True)
    plt.show()

    # 추가 EDA/시각화는 필요에 따라 함수화하여 확장 가능

if __name__ == "__main__":
    main()



