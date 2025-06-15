#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os

def extract_valid_station_ids(json_path: str, output_txt: str) -> None:
    """
    讀取 station_list.json，過濾出 stationEndDate == '' 的 stationID，
    並將去重後的清單寫入 stations_valid.txt。
    """
    # 1. 讀檔
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    # 2. 過濾
    valid_ids = []
    for group in data.get("data", []):
        for st in group.get("item", []):
            # stationEndDate 為空字串，代表尚未撤站
            if not st.get("stationEndDate"):
                sid = st.get("stationID")
                if sid:
                    valid_ids.append(sid)

    # 3. 去重、排序
    valid_ids = sorted(set(valid_ids))

    # 4. 輸出
    os.makedirs(os.path.dirname(output_txt) or ".", exist_ok=True)
    with open(output_txt, "w", encoding="utf-8") as f:
        for sid in valid_ids:
            f.write(sid + "\n")

    print(f"Extracted {len(valid_ids)} valid station IDs → {output_txt}")

if __name__ == "__main__":
    # 如果您的 station_list.json 在同目錄下，就直接指定檔名
    JSON_PATH = "./data/web_api/station_list.json"
    OUTPUT_TXT = "./data/web_api/stations_valid.txt"
    extract_valid_station_ids(JSON_PATH, OUTPUT_TXT)
