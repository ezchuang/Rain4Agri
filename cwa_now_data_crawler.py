import os
import requests
import csv
import json
from datetime import datetime
from typing import List, Dict, Any

# ------------------------------------------------------------------------------
# 請先在系統環境變數中設定 API Key，例如：
#   export CWB_API_KEY="CWB-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
# ------------------------------------------------------------------------------
API_KEY = os.getenv("CWB_API_KEY")
if not API_KEY:
    raise RuntimeError("Please set the environment variable CWB_API_KEY to your CWB API Key.")

BASE_URL = "https://opendata.cwa.gov.tw/api/v1/rest/datastore"
NOW_DATA_FOLDER = "./now_data_github"
os.makedirs(NOW_DATA_FOLDER, exist_ok=True)


def fetch_data(dataset_id: str) -> List[Dict[str, Any]]:
    """
    統一呼叫 CWB Open Data API 的函式，取回指定資料集的所有測站資料。
    :param dataset_id: e.g. "O-A0001-001", "O-A0002-001", "O-A0003-001"
    :return: 回傳 records.Station (list of stations)，若錯誤則拋出例外。
    """
    url = f"{BASE_URL}/{dataset_id}"
    params = {
        "Authorization": API_KEY,
        "format": "JSON"
    }
    resp = requests.get(url, params=params, timeout=10, verify=False)
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to fetch {dataset_id}: HTTP {resp.status_code}")
    data = resp.json()
    # 有些舊版 API 會在 JSON 最外層放入 success 欄位，若 false 也算錯誤
    if isinstance(data.get("success"), bool) and not data["success"]:
        raise RuntimeError(f"API {dataset_id} returned success=false. Message: {data.get('error', {}).get('message')}")
    # 正常結構： data["records"]["Station"] 是 array
    try:
        locations = data["records"]["Station"]
    except KeyError:
        raise RuntimeError(f"Unexpected format: missing records.Station in {dataset_id}")
    return locations


def parse_auto_weather(stations: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    解析 O-A0001-001: 自動氣象站–氣象觀測資料。
    回傳一個 dict，以 stationId 為 key，內容包含 stationName、lon、lat、TEMP、RH、PRES、WDSD、WDIR 等。
    """
    result = {}
    for st in stations:
        sid = st["StationId"]
        # 觀測時間
        timestamp = st["ObsTime"]["DateTime"]
        # 篩選 WGS84 座標
        coords = next((c for c in st["GeoInfo"]["Coordinates"] if c["CoordinateName"]=="WGS84"), {})
        lat = float(coords.get("StationLatitude", 0))
        lon = float(coords.get("StationLongitude", 0))
        we = st["WeatherElement"]
        # 初步組裝基本資訊
        rec = {
            "stationName": st["StationName"],
            "timestamp": timestamp,
            "lat": lat,
            "lon": lon,
            "temperature": float(we.get("AirTemperature", None)),
            "humidity": float(we.get("RelativeHumidity", None)),
            "pressure": float(we.get("AirPressure", None)),
            "wind_speed": float(we.get("WindSpeed", None)),
            "wind_dir": float(we.get("WindDirection", None)),
            "rain": float(we.get("Now", {}).get("Precipitation", None))
        }
        result[sid] = rec
    return result


def parse_auto_rain(stations: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    解析 O-A0002-001 JSON 中的 Station 陣列，
    擷取 WGS84 經緯度、ObsTime、及各時段累積與瞬時降雨量。
    回傳 dict，以 stationId 為 key。
    """
    result = {}
    for st in stations:
        sid = st["StationId"]
        timestamp = st["ObsTime"]["DateTime"]
        # 篩選 WGS84 座標
        coords = next(
            (c for c in st["GeoInfo"]["Coordinates"] if c["CoordinateName"] == "WGS84"),
            {}
        )
        lat = float(coords.get("StationLatitude", 0))
        lon = float(coords.get("StationLongitude", 0))
        rf = st.get("RainfallElement", {})
        rec = {
            "stationName": st["StationName"],
            "timestamp": timestamp,
            "lat": lat,
            "lon": lon,
            # 瞬時雨量
            "rain_now": float(rf.get("Now", {}).get("Precipitation", 0)),
            # 過去各時段累積雨量
            "rain_10min": float(rf.get("Past10Min", {}).get("Precipitation", 0)),
            "rain_1hr":   float(rf.get("Past1hr", {}).get("Precipitation", 0)),
            "rain_3hr":   float(rf.get("Past3hr", {}).get("Precipitation", 0)),
            "rain_6hr":   float(rf.get("Past6Hr", {}).get("Precipitation", 0)),
            "rain_12hr":  float(rf.get("Past12hr", {}).get("Precipitation", 0)),
            "rain_24hr":  float(rf.get("Past24hr", {}).get("Precipitation", 0)),
            "rain_2d":    float(rf.get("Past2days", {}).get("Precipitation", 0)),
            "rain_3d":    float(rf.get("Past3days", {}).get("Precipitation", 0)),
        }
        result[sid] = rec
    return result

def parse_now_weather(stations: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    解析 O-A0003-001 JSON 中的 Station 陣列，
    擷取 WGS84 經緯度、ObsTime，以及所有現況天氣要素。
    回傳 dict，以 stationId 為 key。
    """
    result = {}
    for st in stations:
        sid = st["StationId"]
        timestamp = st["ObsTime"]["DateTime"]
        coords = next(
            (c for c in st["GeoInfo"]["Coordinates"] if c["CoordinateName"] == "WGS84"),
            {}
        )
        lat = float(coords.get("StationLatitude", 0))
        lon = float(coords.get("StationLongitude", 0))
        we = st.get("WeatherElement", {})
        rec = {
            "stationName": st["StationName"],
            "timestamp": timestamp,
            "lat": lat,
            "lon": lon,
            # 常見要素
            "weather":            we.get("Weather"),
            "visibility":         we.get("VisibilityDescription"),
            "sunshine_duration": float(we.get("SunshineDuration", 0)),
            "rain_now":           float(we.get("Now", {}).get("Precipitation", 0)),
            "wind_dir":           float(we.get("WindDirection", 0)),
            "wind_speed":         float(we.get("WindSpeed", 0)),
            "temperature":        float(we.get("AirTemperature", 0)),
            "humidity":           float(we.get("RelativeHumidity", 0)),
            "pressure":           float(we.get("AirPressure", 0)),
            "uv_index":           int(we.get("UVIndex", 0)),
            "peak_gust_speed":    float(we.get("GustInfo", {}).get("PeakGustSpeed", 0)),
            # 如需 Max10MinAverage，可自行擴充：
            # "max10min_avg_speed": float(we.get("Max10MinAverage", {}).get("WindSpeed", 0)),
        }
        result[sid] = rec
    return result

def merge_station_data(
    parsed_aw:   Dict[str, Dict[str, Any]],
    parsed_rain: Dict[str, Dict[str, Any]],
    parsed_now:  Dict[str, Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    將三種資料依 stationId 合併，並將各自的 timestamp 重命名避免衝突。
    回傳 List[Dict]，每筆 dict 為某一 station 的完整資料。
    """
    merged_list: List[Dict[str, Any]] = []
    # 所有會出現的 stationId
    all_sids = set(parsed_aw) | set(parsed_rain) | set(parsed_now)

    for sid in all_sids:
        record: Dict[str, Any] = {}
        # 1. 自動氣象站資料
        aw = parsed_aw.get(sid)
        if aw:
            # 複製一份，避免改到原 dict
            rec_aw = aw.copy()
            # 把 generic timestamp 改名
            rec_aw["timestamp_weather"] = rec_aw.pop("timestamp")
            record.update(rec_aw)

        # 2. 自動雨量站資料
        rain = parsed_rain.get(sid)
        if rain:
            rec_rain = rain.copy()
            rec_rain["timestamp_rain"] = rec_rain.pop("timestamp")
            record.update(rec_rain)

        # 3. 現況天氣資料
        now = parsed_now.get(sid)
        if now:
            rec_now = now.copy()
            rec_now["timestamp_now"] = rec_now.pop("timestamp")
            record.update(rec_now)

        # stationId = key
        record["stationId"] = sid
        merged_list.append(record)

    return merged_list

def save_to_csv(stations: List[Dict[str, Any]], filename: str = "weather_data.csv") -> None:
    """
    將測站資料列表存成 CSV 檔，header 包含所有 record 中出現過的欄位。
    :param stations: merge_station_data 回傳的 List[Dict]
    :param filename: 輸出檔名
    """
    if not stations:
        print("No data to save to CSV.")
        return

    # 1. 蒐集所有欄位
    all_fields = set()
    for rec in stations:
        all_fields.update(rec.keys())
    # 轉成排序後的 list（也可以自己定義順序）
    headers = sorted(all_fields)

    # 2. 寫檔
    with open(os.path.join(NOW_DATA_FOLDER, filename), mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        for station in stations:
            # 確保每個欄位都有值，缺的就填 None 或空字串
            row = {key: station.get(key, "") for key in headers}
            writer.writerow(row)

    print(f"Saved data to CSV file: {filename}")


def save_to_json(stations: List[Dict[str, Any]], filename: str = "weather_data.json") -> None:
    """
    將測站資料列表存成 JSON 檔。
    :param stations: merge_station_data 回傳的 List[Dict]
    :param filename: 輸出檔名
    """
    with open(os.path.join(NOW_DATA_FOLDER, filename), mode="w", encoding="utf-8") as jsonfile:
        json.dump(stations, jsonfile, ensure_ascii=False, indent=2)

    print(f"Saved data to JSON file: {filename}")


def main():
    try:
        # 1. 取得自動氣象站資料 (每小時更新一次) :contentReference[oaicite:20]{index=20}
        aw_locations = fetch_data("O-A0001-001")
        parsed_aw = parse_auto_weather(aw_locations)

        # 2. 取得自動雨量站資料 (每 10 分鐘更新一次) :contentReference[oaicite:21]{index=21}
        rain_locations = fetch_data("O-A0002-001")
        parsed_rain = parse_auto_rain(rain_locations)

        # 3. 取得現在天氣觀測報告 (含雲量，同樣每 10 分鐘更新) :contentReference[oaicite:22]{index=22}
        cloud_locations = fetch_data("O-A0003-001")
        parsed_cloud = parse_now_weather(cloud_locations)

        # 4. 合併資料
        merged_list = merge_station_data(parsed_aw, parsed_rain, parsed_cloud)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")  

        csv_name = f"taiwan_weather_{ts}.csv"
        json_name = f"taiwan_weather_{ts}.json"

        # 5. 儲存資料到檔案
        save_to_csv(merged_list, filename=csv_name)
        save_to_json(merged_list, filename=json_name)

    except Exception as e:
        print("Error:", e)
        # 若任何 API 無法正確取得，就在此印出錯誤訊息並退出
        return


if __name__ == "__main__":
    main()
