"""
거리 계산 유틸리티 모듈
지리적 거리 계산을 위한 다양한 함수들을 제공합니다.
"""

import math
import numpy as np
from haversine import haversine
from sklearn.neighbors import BallTree
import pandas as pd
from typing import Tuple, Union, List


class DistanceCalculator:
    """지리적 거리 계산을 위한 클래스"""
    
    def __init__(self, earth_radius_km: float = 6371):
        """
        Args:
            earth_radius_km: 지구 반지름 (km), 기본값 6371km
        """
        self.earth_radius_km = earth_radius_km
        self.earth_radius_m = earth_radius_km * 1000
    
    def haversine_distance(self, lat1: float, lon1: float, 
                          lat2: float, lon2: float, unit: str = 'km') -> float:
        """
        Haversine 공식을 사용한 두 지점 간 거리 계산
        
        Args:
            lat1, lon1: 첫 번째 지점의 위도, 경도
            lat2, lon2: 두 번째 지점의 위도, 경도
            unit: 반환 단위 ('km' 또는 'm')
            
        Returns:
            두 지점 간 거리
        """
        distance_km = haversine((lat1, lon1), (lat2, lon2), unit='km')
        return distance_km if unit == 'km' else distance_km * 1000
    
    def euclidean_distance_3d(self, lat1: float, lon1: float, 
                             lat2: float, lon2: float, unit: str = 'm') -> float:
        """
        3D 유클리드 거리 계산 (지구를 구로 가정)
        
        Args:
            lat1, lon1: 첫 번째 지점의 위도, 경도
            lat2, lon2: 두 번째 지점의 위도, 경도
            unit: 반환 단위 ('km' 또는 'm')
            
        Returns:
            두 지점 간 3D 유클리드 거리
        """
        # 위경도를 라디안으로 변환
        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)
        
        # 3D 직교 좌표로 변환
        x1 = self.earth_radius_m * math.cos(lat1_rad) * math.cos(lon1_rad)
        y1 = self.earth_radius_m * math.cos(lat1_rad) * math.sin(lon1_rad)
        z1 = self.earth_radius_m * math.sin(lat1_rad)
        
        x2 = self.earth_radius_m * math.cos(lat2_rad) * math.cos(lon2_rad)
        y2 = self.earth_radius_m * math.cos(lat2_rad) * math.sin(lon2_rad)
        z2 = self.earth_radius_m * math.sin(lat2_rad)
        
        # 3D 유클리드 거리 계산
        distance_m = math.sqrt((x2 - x1)**2 + (y2 - y1)**2 + (z2 - z1)**2)
        
        return distance_m if unit == 'm' else distance_m / 1000
    
    def count_nearby_stores(self, df_base: pd.DataFrame, df_stores: pd.DataFrame, 
                           distance_km: float = 0.7, lat_col: str = '위도', 
                           lon_col: str = '경도') -> np.ndarray:
        """
        각 기준점에서 지정된 반경 내 상점 개수 계산
        
        Args:
            df_base: 기준점들이 있는 DataFrame
            df_stores: 상점들이 있는 DataFrame
            distance_km: 반경 (km)
            lat_col, lon_col: 위도, 경도 컬럼명
            
        Returns:
            각 기준점별 반경 내 상점 개수 배열
        """
        # 좌표를 라디안으로 변환
        store_coords = np.radians(df_stores[[lat_col, lon_col]])
        base_coords = np.radians(df_base[[lat_col, lon_col]])
        
        # BallTree 구성
        tree = BallTree(store_coords, metric='haversine')
        
        # 반경을 라디안으로 변환
        radius_rad = distance_km / self.earth_radius_km
        
        # 각 기준점의 반경 내 상점 개수 계산
        counts = tree.query_radius(base_coords, r=radius_rad, count_only=True)
        
        return counts
    
    def find_closest_points(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                           target_col: str, lat_col: str = '위도', 
                           lon_col: str = '경도') -> Tuple[np.ndarray, pd.Series]:
        """
        df1의 각 점에서 가장 가까운 df2의 점과 거리 찾기
        
        Args:
            df1: 기준점들이 있는 DataFrame
            df2: 대상점들이 있는 DataFrame
            target_col: df2에서 가져올 컬럼명
            lat_col, lon_col: 위도, 경도 컬럼명
            
        Returns:
            (거리 배열, 가장 가까운 점의 target_col 값)
        """
        # 좌표를 라디안으로 변환
        coords1 = np.radians(df1[[lat_col, lon_col]].to_numpy())
        coords2 = np.radians(df2[[lat_col, lon_col]].to_numpy())
        
        # BallTree 구성
        tree = BallTree(coords2, metric='haversine')
        
        # 가장 가까운 점 찾기
        distances, indices = tree.query(coords1, k=1)
        
        # 거리를 km로 변환
        distances_km = distances.flatten() * self.earth_radius_km
        
        # 가장 가까운 점의 target_col 값 추출
        closest_values = df2.iloc[indices.flatten()][target_col].reset_index(drop=True)
        
        return distances_km, closest_values
    
    def calculate_all_nearby_features(self, df_base: pd.DataFrame, 
                                    df_stores: pd.DataFrame, 
                                    distance_km: float = 0.7,
                                    lat_col: str = '위도', 
                                    lon_col: str = '경도') -> pd.DataFrame:
        """
        기준점들에 대해 모든 근처 상점 관련 특성 계산
        
        Args:
            df_base: 기준점들이 있는 DataFrame
            df_stores: 상점들이 있는 DataFrame
            distance_km: 반경 (km)
            lat_col, lon_col: 위도, 경도 컬럼명
            
        Returns:
            근처 상점 특성이 추가된 DataFrame
        """
        df_result = df_base.copy()
        
        # 전체 상점 수
        df_result['근방가게수'] = self.count_nearby_stores(
            df_base, df_stores, distance_km, lat_col, lon_col
        )
        
        # 음식점 수
        df_restaurants = df_stores[df_stores['상권업종대분류명'] == '음식']
        if not df_restaurants.empty:
            df_result['근방음식점수'] = self.count_nearby_stores(
                df_base, df_restaurants, distance_km, lat_col, lon_col
            )
        else:
            df_result['근방음식점수'] = 0
        
        # 카페 수
        df_cafes = df_stores[df_stores['상권업종소분류명'] == '카페']
        if not df_cafes.empty:
            df_result['근방카페수'] = self.count_nearby_stores(
                df_base, df_cafes, distance_km, lat_col, lon_col
            )
        else:
            df_result['근방카페수'] = 0
        
        return df_result


# 편의 함수들
def quick_distance(lat1: float, lon1: float, lat2: float, lon2: float, 
                  method: str = 'haversine', unit: str = 'km') -> float:
    """
    빠른 거리 계산 함수
    
    Args:
        lat1, lon1, lat2, lon2: 두 지점의 위도, 경도
        method: 계산 방법 ('haversine' 또는 'euclidean')
        unit: 반환 단위 ('km' 또는 'm')
        
    Returns:
        두 지점 간 거리
    """
    calculator = DistanceCalculator()
    
    if method == 'haversine':
        return calculator.haversine_distance(lat1, lon1, lat2, lon2, unit)
    elif method == 'euclidean':
        return calculator.euclidean_distance_3d(lat1, lon1, lat2, lon2, unit)
    else:
        raise ValueError("method는 'haversine' 또는 'euclidean'이어야 합니다")


def batch_distance_calculation(df1: pd.DataFrame, df2: pd.DataFrame, 
                             target_col: str, lat_col: str = '위도', 
                             lon_col: str = '경도') -> Tuple[np.ndarray, pd.Series]:
    """
    배치 거리 계산 함수
    
    Args:
        df1, df2: 두 DataFrame
        target_col: df2에서 가져올 컬럼명
        lat_col, lon_col: 위도, 경도 컬럼명
        
    Returns:
        (거리 배열, 가장 가까운 점의 target_col 값)
    """
    calculator = DistanceCalculator()
    return calculator.find_closest_points(df1, df2, target_col, lat_col, lon_col)