import os
import requests
import json
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
# from typing import List, Dict
import time
import random
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 參數設定 ---
BASE_API       = "https://codis.cwa.gov.tw/api/station?"
OUTPUT_ROOT    = "./data/his_data"
STATIONS_FILE  = "./data/web_api/stations_valid.txt"
STATION_LIST   = "./data/web_api/station_list.json"
DAYS_BACK      = 180

# 每次請求之間隨機等待的秒數範圍 (避免被 Ban)
REQUEST_DELAY_RANGE = (0.5, 1.5)   # 0.5 ~ 1.5 秒
# 每跑完一個站點之後再多等一下
STATION_DELAY_RANGE = (5, 10)      # 5 ~ 10 秒

FIELDS = {
    "station_pressure":      "Station Pressure (hPa)",       # 測站氣壓(hPa)
    "sea_level_pressure":    "Sea Level Pressure (hPa)",    # 海平面氣壓(hPa)
    "air_temperature":       "Air Temperature (°C)",        # 氣溫(°C)
    "dew_point_temperature": "Dew Point Temperature (°C)",  # 露點溫度(°C)
    "relative_humidity":     "Relative Humidity (%)",       # 相對溼度(%)
    "wind_speed":            "Wind Speed (m/s)",            # 風速(m/s)
    "wind_direction":        "Wind Direction (360°)",       # 風向(360°)
    "max_gust_speed":        "Max Gust Speed (m/s)",        # 最大瞬間風(m/s)
    "max_gust_direction":    "Max Gust Direction (360°)",   # 最大瞬間風風向(360°)
    "precipitation":         "Precipitation (mm)",          # 降水量(mm)
    "precipitation_hours":   "Precipitation Hours (hour)",  # 降水時數(hour)
    "sunshine_hours":        "Sunshine Hours (hour)",       # 日照時數(hour)
    "solar_radiation":       "Solar Radiation (MJ/m²)",     # 全天空日射量(MJ/㎡)
    "visibility":            "Visibility (km)",             # 能見度(km)
    "uv_index":              "UV Index",                    # 紫外線指數
    "total_cloud_cover":     "Total Cloud Cover (0–10)",    # 總雲量(0~10)
    "soil_temp_0cm":         "Soil Temperature 0 cm",       # 地溫0cm
    "soil_temp_5cm":         "Soil Temperature 5 cm",       # 地溫5cm
    "soil_temp_10cm":        "Soil Temperature 10 cm",      # 地溫10cm
    "soil_temp_20cm":        "Soil Temperature 20 cm",      # 地溫20cm
    "soil_temp_30cm":        "Soil Temperature 30 cm",      # 地溫30cm
    "soil_temp_50cm":        "Soil Temperature 50 cm",      # 地溫50cm
}

# 載入 station_list, 建立 stationID → stn_type (stationAttribute) map
with open(STATION_LIST, encoding="utf-8") as f:
    raw = json.load(f)
stn_type_map = {}
for group in raw["data"]:
    stn_type = group["stationAttribute"]    # e.g. "agr","auto","cwb"
    for st in group["item"]:
        if not st.get("stationEndDate"):    # 只要「未撤站」的
            stn_type_map[st["stationID"]] = stn_type

def download_hourly_csv_api(station_id: str, date_str: str) -> pd.DataFrame:
    """
    直接呼叫隱藏 API，下載某站當天逐時資料的 CSV，並回傳 DataFrame。
    """
    # 找對應的 stn_type
    stn_type = stn_type_map.get(station_id)
    if not stn_type:
        raise RuntimeError(f"找不到 station_id={station_id} 的 stn_type，請先檢查 station_list.json")
    
    payload = {
        "date":     f"{date_str}T00:00:00.000+08:00",
        "type":     "report_date",
        "stn_ID":   station_id,
        "stn_type": stn_type,
        "start":    f"{date_str}T00:00:00",
        "end":      f"{date_str}T23:59:59",
        "item":     ""                         # 空字串代表全欄位
    }
    headers = {"Accept": "text/csv"}
    resp = requests.post(
        BASE_API,
        data=payload,
        headers=headers,
        timeout=30,
        verify=False      # << 停用 SSL certificate 驗證
    )
    resp.raise_for_status()
    df = pd.read_csv(StringIO(resp.text))
    return df

def ensure_dir(path: str):
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)

def main():
    # 1. 讀取所有有效 station ID
    with open(STATIONS_FILE, encoding="utf-8") as f:
        stations = [s.strip() for s in f if s.strip()]

    # 2. 產生過去 180 天的日期清單
    dates = [
        (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(DAYS_BACK)
    ]

    # 3. 依站、依日進行下載並拆欄儲存
    for sid in stations:
        print(f"### 開始站點 {sid} ###")
        for d in dates:
            try:
                df = download_hourly_csv_api(sid, d)
                # 請求之後隨機 delay
                time.sleep(random.uniform(*REQUEST_DELAY_RANGE))
            except Exception as e:
                print(f"[{sid}][{d}] API 下載失敗：{e}")
                # 錯誤後也稍微休息再繼續
                time.sleep(random.uniform(*REQUEST_DELAY_RANGE))
                continue

            # 拆欄並存檔
            for key, colname in FIELDS.items():
                if colname not in df.columns:
                    continue
                out_dir = os.path.join(OUTPUT_ROOT, sid, key)
                ensure_dir(out_dir)
                out_path = os.path.join(out_dir, f"{d}.csv")
                
                df[[df.columns[0], colname]].to_csv(
                    out_path, index=False, encoding="utf-8-sig"
                )
                print(f"[{sid}][{d}] 存檔 → {key}/{d}.csv")

        # 每跑完一個站，再做較長的 delay，避免過度集中請求
        station_delay = random.uniform(*STATION_DELAY_RANGE)
        print(f"--- 站點 {sid} 完成，休息 {station_delay:.1f} 秒 ---")
        time.sleep(station_delay)

if __name__ == "__main__":
    main()
