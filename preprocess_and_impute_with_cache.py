#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
名稱：preprocess_impute_with_cache.py

功能：
1. 展平 data/his_data/<StationID>/*.json，去除 -99.5、-99.95 佔位值。
2. 合併 data/web_api/stations_valid.txt 與 station_list.json（經緯度、海拔）。
3. 計算並快取每測站鄰站清單到 data/web_api/station_neighbors.json。
4. 使用 ProcessPoolExecutor (80% CPU) 以 3D IDW (Power=2, 取最近 MIN_NEIGHBORS 個鄰站) 插補空值，並記錄日誌。
5. 支援 Windows 多進程啟動，加入 freeze_support()。
6. 輸出：
   - 原始清洗：data/cleaned_initial_data.csv
   - 插補後：  data/cleaned_initial_data_imputed.csv
   - 快取鄰站： data/web_api/station_neighbors.json
   - 日誌：      data/web_api/preprocess_impute.log
"""

import os
import json
import pandas as pd
import numpy as np
from math import radians, sin, cos, asin, sqrt
from concurrent.futures import ProcessPoolExecutor
import multiprocessing

# --- 路徑設定 ---
HIS_ROOT         = 'data/his_data'
STATIONS_VALID   = 'data/web_api/stations_valid.txt'
STATION_LIST     = 'data/web_api/station_list.json'
NEIGHBORS_CACHE  = 'data/web_api/station_neighbors.json'
CLEANED_CSV      = 'data/cleaned_initial_data.csv'
IMPUTED_CSV      = 'data/cleaned_initial_data_imputed.csv'
LOG_PATH         = 'data/logs/preprocess_impute.log'

# --- 插補參數 ---
MIN_NEIGHBORS = 3
IDW_POWER     = 2

# --- 欄位結構定義 ---
fields = {
    "StationPressure": ["Instantaneous"],
    "SeaLevelPressure": ["Instantaneous"],
    "AirTemperature": ["Instantaneous","Maximum","Minimum"],
    "DewPointTemperature": ["Instantaneous"],
    "RelativeHumidity": ["Instantaneous"],
    "WindSpeed": ["TenMinutelyMaximum","Mean"],
    "WindDirection": ["TenMinutelyMaximum","Mean"],
    "PeakGust": ["Direction","Maximum"],
    "Precipitation": ["Accumulation"],
    "PrecipitationDuration": ["Total"],
    "SunshineDuration": ["Total"],
    "GlobalSolarRadiation": ["Accumulation"],
    "Visibility": ["Instantaneous"],
    "UVIndex": ["Accumulation"],
    "TotalCloudAmount": ["Instantaneous"],
    **{f"SoilTemperatureAt{d}cm": ["Instantaneous"] for d in [0,5,10,20,30,50,100]}
}

def flatten_and_clean(valid_stations):
    records = []
    for sid in sorted(valid_stations):
        station_dir = os.path.join(HIS_ROOT, sid)
        if not os.path.isdir(station_dir):
            continue
        for fname in sorted(os.listdir(station_dir)):
            if not fname.endswith('.json'):
                continue
            with open(os.path.join(station_dir, fname), 'r', encoding='utf-8') as f:
                data = json.load(f)
            items = []
            if isinstance(data, dict) and 'data' in data:
                items = data['data']
            elif isinstance(data, list):
                items = data
            elif isinstance(data, dict) and 'StationID' in data and 'dts' in data:
                items = [data]
            for item in items:
                dts = item.get('dts') or item.get('data', [])
                for entry in dts:
                    row = {'StationID': sid, 'DataTime': entry.get('DataTime')}
                    for grp, subs in fields.items():
                        grp_data = entry.get(grp, {}) or {}
                        for sub in subs:
                            col = f"{grp}_{sub}"
                            val = grp_data.get(sub)
                            if isinstance(val, (int, float)) and val in (
                                -9.5, -99.5, -99.9, -99.95, -999.5, -9999.5, 
                                -9.9500, -9995.0000, -9.8000, -9995.0, -9.5000, -99.7000, -99.9000
                                ):
                                val = np.nan
                            row[col] = val
                    records.append(row)
    df0 = pd.DataFrame(records)
    os.makedirs(os.path.dirname(CLEANED_CSV), exist_ok=True)
    df0.to_csv(CLEANED_CSV, index=False)
    return df0

def load_station_info(valid_stations):
    with open(STATION_LIST, 'r', encoding='utf-8') as f:
        meta = json.load(f)
    station_info = {}
    station_meta = []
    for grp in meta.get('data', []):
        for item in grp.get('item', []):
            sid = item.get('stationID')
            if sid in valid_stations:
                lon = item.get('longitude', np.nan)
                lat = item.get('latitude', np.nan)
                alt = item.get('altitude', np.nan)
                station_info[sid] = {'lon': lon, 'lat': lat, 'alt': alt}
                station_meta.append({'StationID': sid, 'Longitude': lon, 'Latitude': lat, 'Altitude': alt})
    station_meta_df = pd.DataFrame(station_meta)
    return station_info, station_meta_df

def compute_and_cache_neighbors(station_info):
    def distance_3d(a, b):
        i1, i2 = station_info[a], station_info[b]
        lon1, lat1, lon2, lat2 = map(radians, [i1['lon'], i1['lat'], i2['lon'], i2['lat']])
        dxy = 2*asin(sqrt(sin((lat2-lat1)/2)**2 + cos(lat1)*cos(lat2)*sin((lon2-lon1)/2)**2))*6371
        dz = abs(i1['alt'] - i2['alt'])/1000.0 if not np.isnan(i1['alt']) and not np.isnan(i2['alt']) else 0
        return sqrt(dxy**2 + dz**2)
    neighbors = {}
    for sid in station_info:
        dists = [(o, distance_3d(sid, o)) for o in station_info if o != sid]
        dists.sort(key=lambda x: x[1])
        neighbors[sid] = [{'station': o, 'distance_km': round(d,4)} for o,d in dists]
    os.makedirs(os.path.dirname(NEIGHBORS_CACHE), exist_ok=True)
    with open(NEIGHBORS_CACHE, 'w', encoding='utf-8') as cf:
        json.dump(neighbors, cf, ensure_ascii=False, indent=2)
    return neighbors

def impute_slice(args):
    t, grp, neighbors = args
    proc = multiprocessing.current_process().name
    grp = grp.copy()
    for station in grp.index:
        for col in grp.columns:
            if pd.isna(grp.at[station, col]):
                avail = []
                for n in neighbors.get(station, []):
                    s2, dist = n['station'], n['distance_km']
                    if s2 not in grp.index:
                        continue
                    val = grp.at[s2, col]
                    if pd.notna(val):
                        avail.append((s2, dist))
                    if len(avail) >= MIN_NEIGHBORS:
                        break
                if len(avail) < MIN_NEIGHBORS:
                    with open(LOG_PATH, 'a') as log:
                        log.write(f"[{t}][{proc}] {station}/{col} nbr<{MIN_NEIGHBORS}\n")
                else:
                    vals  = [grp.at[s, col] for s,_ in avail]
                    dists = [d   for _,d in avail]
                    w     = [1/(d**IDW_POWER) if d>0 else 0 for d in dists]
                    grp.at[station, col] = sum(v*wi for v,wi in zip(vals, w)) / sum(w)
    return grp.reset_index()

def main():
    multiprocessing.freeze_support()
    with open(STATIONS_VALID, 'r', encoding='utf-8') as f:
        valid_stations = {line.strip() for line in f if line.strip()}
    df0 = flatten_and_clean(valid_stations)
    station_info, station_meta_df = load_station_info(valid_stations)
    neighbors = compute_and_cache_neighbors(station_info)

    feature_cols = [c for c in df0.columns if c not in ('StationID','DataTime')]
    groups = [(t, g.set_index('StationID')[feature_cols], neighbors)
              for t, g in df0.groupby('DataTime')]

    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    open(LOG_PATH, 'w').close()

    num_procs = max(1, int(multiprocessing.cpu_count() * 0.8))
    with ProcessPoolExecutor(max_workers=num_procs) as exe:
        results = list(exe.map(impute_slice, groups))

    imputed = pd.concat([g.assign(DataTime=t) for (t,_,_), g in zip(groups, results)], ignore_index=True)
    imputed = imputed.merge(station_meta_df, on='StationID', how='left')
    os.makedirs(os.path.dirname(IMPUTED_CSV), exist_ok=True)
    imputed.to_csv(IMPUTED_CSV, index=False)

    print("預處理與多進程插補完成。")

if __name__ == '__main__':
    main()
