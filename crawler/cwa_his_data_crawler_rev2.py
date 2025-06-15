import os
import json
import time
import requests
from datetime import datetime, timedelta

# --- 設定區 ---
STATIONS_FILE  = "./data/web_api/stations_valid.txt"
STATION_LIST   = "./data/web_api/station_list.json"
BASE_API       = "https://codis.cwa.gov.tw/api/station?"
OUTPUT_ROOT    = "./data/his_data"
DAYS_BACK     = 180    # 往前抓半年
DELAY_SECONDS = 0.0    # 每次請求後暫停 1 秒
STATION_DELAY = 0.0    # 每換一個 station 暫停 5 秒
# ---------------

# 載入 station_list, 建立 stationID → stn_type (stationAttribute) map
with open(STATION_LIST, encoding="utf-8") as f:
    raw = json.load(f)
stn_type_map = {}
for group in raw["data"]:
    stn_type = group["stationAttribute"]    # e.g. "agr","auto","cwb"
    for st in group["item"]:
        if not st.get("stationEndDate"):    # 只要「未撤站」的
            stn_type_map[st["stationID"]] = stn_type

def fetch_day_json(station_id: str, date_str: str) -> dict:
    # 找對應的 stn_type
    stn_type = stn_type_map.get(station_id)
    if not stn_type:
        raise RuntimeError(f"找不到 station_id={station_id} 的 stn_type，請先檢查 station_list.json")
    
    payload = {
        "date": f"{date_str}T00:00:00.000+08:00",
        "type": "report_date",
        "stn_ID": station_id,
        "stn_type": stn_type,
        "start": f"{date_str}T00:00:00",
        "end": f"{date_str}T23:59:59",
        "item": ""
    }
    headers = {"Accept": "text/csv,application/json"}
    # resp = requests.post(BASE_API, data=payload, timeout=30)
    resp = requests.post(
        BASE_API,
        data=payload,
        headers=headers,
        timeout=30,
        verify=False      # << 停用 SSL certificate 驗證
    )
    resp.raise_for_status()
    return resp.json()

def main():
    # 1. 讀有效 station_id
    with open(STATIONS_FILE, encoding="utf-8") as f:
        station_ids = [line.strip() for line in f if line.strip()]

    # 2. 產生日期清單
    today = datetime.now()
    dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(DAYS_BACK)]

    # 3. 逐站逐日呼叫並存 JSON
    for sid in station_ids:
        print(f"=== Station {sid} ===")
        station_folder = os.path.join(OUTPUT_ROOT, sid)
        os.makedirs(station_folder, exist_ok=True)

        for d in dates:
            out_path = os.path.join(station_folder, f"{d}.json")
            if os.path.isfile(out_path):
                continue

            try:
                data = fetch_day_json(sid, d)
            except Exception as e:
                print(f"[{d}] 下載失敗：{e}")
            else:
                with open(out_path, "w", encoding="utf-8") as fp:
                    json.dump(data, fp, ensure_ascii=False, indent=2)
                print(f"[{d}] 儲存→ {out_path}")

            # 單日請求後暫停
            time.sleep(DELAY_SECONDS)

        # 換下一個 station 前，再多暫停一下
        print(f"=== {sid} 完成，暫停 {STATION_DELAY} 秒後繼續 ===")
        time.sleep(STATION_DELAY)

if __name__ == "__main__":
    main()
