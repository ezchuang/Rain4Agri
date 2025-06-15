#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from datetime import datetime, timedelta
from owslib.wms import WebMapService

# --- 參數設定 ---
# 影像圖層：可見光真彩（也可換成其他 Cloud/MODIS/VIIRS 圖層）
LAYER = "VIIRS_SNPP_CorrectedReflectance_TrueColor"
# WMS 端點（EPSG:4326）
WMS_URL = "https://gibs.earthdata.nasa.gov/wms/epsg4326/best/wms.cgi"
# 台灣大致經緯度範圍：[minLon, minLat, maxLon, maxLat]
BBOX_TAIWAN = (119.5, 21.5, 122.5, 25.5)
# 輸出資料夾
OUTPUT_DIR = "./data/satellite"
# 往前抓取天數、每隔幾小時取一張
DAYS_BACK      = 180
INTERVAL_HOURS = 3
IMAGE_SIZE     = (1024, 1024)  # 輸出影像解析度

def main():
    # 建立 WMS 連線
    wms = WebMapService(WMS_URL)
    # 時間範圍
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=DAYS_BACK)
    current = start_time

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 依時間跳步下載
    while current <= end_time:
        # GIBS 要求 ISO8601 UTC，例如 "2025-06-07T12:00:00Z"
        time_str = current.strftime("%Y-%m-%dT%H:%M:%SZ")
        filename = current.strftime("taiwan_%Y%m%d_%H%M.png")
        out_path = os.path.join(OUTPUT_DIR, filename)

        print(f"[{time_str}] 下載 → {filename}")
        try:
            img = wms.getmap(
                layers=[LAYER],
                styles=[""],              # 真彩不用特定樣式
                srs="EPSG:4326",
                bbox=BBOX_TAIWAN,
                size=IMAGE_SIZE,
                format="image/png",
                transparent=True,
                time=time_str            # TIME KVP
            )
            with open(out_path, "wb") as f:
                f.write(img.read())
        except Exception as e:
            print(f"  下載失敗：{e}")

        # 時間遞增
        current += timedelta(hours=INTERVAL_HOURS)

if __name__ == "__main__":
    main()