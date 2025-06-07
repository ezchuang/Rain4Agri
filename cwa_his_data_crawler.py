import os
import time
from io import StringIO
import requests
import pandas as pd
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- 參數設定 ---
OUTPUT_ROOT = "./data/his_data"     # 最上層資料夾
STATIONS_FILE = "./data/web_api/stations_valid.txt"  # 內含所有 StationId，一行一個
DAYS_BACK = 180                 # 往前推半年
BASE_URL = "https://codis.cwa.gov.tw/StationData"

# 你要拆出的欄位名稱（對應 CSV 中的 header）
FIELDS = {
    "station_pressure":        "Station Pressure (hPa)",       # 測站氣壓(hPa)
    "sea_level_pressure":      "Sea Level Pressure (hPa)",    # 海平面氣壓(hPa)
    "air_temperature":         "Air Temperature (°C)",        # 氣溫(°C)
    "dew_point_temperature":   "Dew Point Temperature (°C)",  # 露點溫度(°C)
    "relative_humidity":       "Relative Humidity (%)",       # 相對溼度(%)
    "wind_speed":              "Wind Speed (m/s)",            # 風速(m/s)
    "wind_direction":          "Wind Direction (360°)",       # 風向(360degree)
    "max_gust_speed":          "Max Gust Speed (m/s)",        # 最大瞬間風(m/s)
    "max_gust_direction":      "Max Gust Direction (360°)",   # 最大瞬間風風向(360degree)
    "precipitation":           "Precipitation (mm)",          # 降水量(mm)
    "precipitation_hours":     "Precipitation Hours (hour)",  # 降水時數(hour)
    "sunshine_hours":          "Sunshine Hours (hour)",       # 日照時數(hour)
    "solar_radiation":         "Solar Radiation (MJ/m²)",     # 全天空日射量(MJ/㎡)
    "visibility":              "Visibility (km)",             # 能見度(km)
    "uv_index":                "UV Index",                    # 紫外線指數
    "total_cloud_cover":       "Total Cloud Cover (0–10)",    # 總雲量(0~10)
    "soil_temp_0cm":           "Soil Temperature 0 cm",       # 地溫0cm
    "soil_temp_5cm":           "Soil Temperature 5 cm",       # 地溫5cm
    "soil_temp_10cm":          "Soil Temperature 10 cm",      # 地溫10cm
    "soil_temp_20cm":          "Soil Temperature 20 cm",      # 地溫20cm
    "soil_temp_30cm":          "Soil Temperature 30 cm",      # 地溫30cm
    "soil_temp_50cm":          "Soil Temperature 50 cm",      # 地溫50cm
}

# Selenium 初始化 (headless)
options = webdriver.ChromeOptions()
# options.add_argument("--headless")
driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=options
)
wait = WebDriverWait(driver, 20)

def download_hourly_csv(station_id: str, date_str: str) -> pd.DataFrame:
    """
    針對某站某日下載「逐時資料」CSV，並回傳 pandas.DataFrame
    """
    # 1. 開啟頁面
    driver.get(BASE_URL)

    # 2. 輸入站號、選第一筆
    inp = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[placeholder*='站名/站號']")))
    inp.clear()
    inp.send_keys(station_id)
    opt = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".station-list li")))
    opt.click()

    # 3. 點「觀看時序圖表」
    wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'觀看時序圖表')]"))).click()

    # 4. 切到「逐時資料」標籤 (假設第1個 tab)
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".nav-tabs li:nth-child(1)"))).click()

    # 5. 填入日期 (YYYY-MM-DD)
    date_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input.datepicker")))
    driver.execute_script("arguments[0].removeAttribute('readonly')", date_input)
    date_input.clear()
    date_input.send_keys(date_str + "\n")
    time.sleep(1)  # 等待資料刷新

    # 6. 取得 CSV 下載連結
    csv_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(text(),'CSV下載')]")))
    csv_url = csv_btn.get_attribute("href")

    # 7. 下載 CSV
    if not csv_url:
        raise RuntimeError(f"CSV download URL({csv_url}) not found")
    resp = requests.get(csv_url, timeout=30)
    resp.raise_for_status()
    df = pd.read_csv(StringIO(resp.text))
    return df

def ensure_dir(path: str):
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)

def main():
    # 讀站號清單
    with open(STATIONS_FILE, encoding="utf-8") as f:
        stations = [line.strip() for line in f if line.strip()]

    # 日期列表：從今天往前 DAYS_BACK 天
    dates = [(datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(DAYS_BACK)]

    for sid in stations:
        print(f"=== Station {sid} ===")
        for date_str in dates:
            try:
                df = download_hourly_csv(sid, date_str)
            except Exception as e:
                print(f"[{date_str}] 下載失敗：{e}")
                continue

            # 依 FIELDS 拆欄並存檔
            for key, colname in FIELDS.items():
                if colname not in df.columns:
                    print(f"  欄位 {colname} 不存在，跳過")
                    continue

                folder = os.path.join(OUTPUT_ROOT, sid, key)
                ensure_dir(folder)
                out_path = os.path.join(folder, f"{date_str}.csv")

                # 只保留時間跟該欄位
                df_sub = df[[df.columns[0], colname]]  # 假設第1欄是時間戳
                df_sub.to_csv(out_path, index=False, encoding="utf-8-sig")
                print(f"  [{date_str}] 存 {key} -> {out_path}")

    driver.quit()

if __name__ == "__main__":
    main()
