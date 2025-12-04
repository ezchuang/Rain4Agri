import os
import json
import pandas as pd
from datetime import datetime

# ---------- 配置 ----------
HIS_ROOT        = 'data/his_data'
CLEANED_CSV     = 'data/cleaned_initial_data.csv'
IMPUTED_CSV     = 'data/cleaned_initial_data_imputed.csv'
OUTPUT_SUMMARY  = 'data/report_summary.csv'

# ---------- 原始 JSON 資料統計 ----------
raw_stations = [d for d in os.listdir(HIS_ROOT) if os.path.isdir(os.path.join(HIS_ROOT, d))]
raw_num_stations = len(raw_stations)
raw_num_files = 0
raw_records = 0
raw_datetimes = []

for sid in raw_stations:
    station_dir = os.path.join(HIS_ROOT, sid)
    for fname in os.listdir(station_dir):
        if not fname.endswith('.json'):
            continue
        raw_num_files += 1
        with open(os.path.join(station_dir, fname), 'r', encoding='utf-8') as f:
            data = json.load(f)
        # 支援多種結構
        items = []
        if isinstance(data, dict) and 'data' in data:
            items = data['data']
        elif isinstance(data, list):
            items = data
        elif isinstance(data, dict) and 'dts' in data:
            items = [data]
        for item in items:
            for entry in item.get('dts', []):
                raw_records += 1
                raw_datetimes.append(entry.get('DataTime'))

raw_start = min(raw_datetimes) if raw_datetimes else None
raw_end   = max(raw_datetimes) if raw_datetimes else None

# ---------- 清洗後資料統計 ----------
df_clean = pd.read_csv(CLEANED_CSV, parse_dates=['DataTime'])
cleaned_records  = len(df_clean)
cleaned_stations = df_clean['StationID'].nunique()
cleaned_columns  = df_clean.shape[1]
cleaned_start    = df_clean['DataTime'].min()
cleaned_end      = df_clean['DataTime'].max()

# ---------- 插補後資料統計 ----------
df_imp = pd.read_csv(IMPUTED_CSV, parse_dates=['DataTime'])
imputed_records  = len(df_imp)
imputed_stations = df_imp['StationID'].nunique()
imputed_columns  = df_imp.shape[1]
imputed_start    = df_imp['DataTime'].min()
imputed_end      = df_imp['DataTime'].max()

# ---------- 匯總並輸出 ----------
summary = {
    'Stage': [
        'Raw JSON',
        'Cleaned CSV',
        'Imputed CSV'
    ],
    'NumStations': [
        raw_num_stations,
        cleaned_stations,
        imputed_stations
    ],
    'NumFiles/Cols': [
        raw_num_files,
        cleaned_columns,
        imputed_columns
    ],
    'NumRecords': [
        raw_records,
        cleaned_records,
        imputed_records
    ],
    'TimeStart': [
        raw_start,
        cleaned_start,
        imputed_start
    ],
    'TimeEnd': [
        raw_end,
        cleaned_end,
        imputed_end
    ]
}

df_summary = pd.DataFrame(summary)
os.makedirs(os.path.dirname(OUTPUT_SUMMARY), exist_ok=True)
df_summary.to_csv(OUTPUT_SUMMARY, index=False, encoding='utf-8')

# 列印 Markdown 表格
print("| Stage       | NumStations | NumFiles/Cols | NumRecords | TimeStart           | TimeEnd             |")
print("|-------------|-------------|---------------|------------|---------------------|---------------------|")
for _, row in df_summary.iterrows():
    print(f"| {row.Stage} | {row.NumStations} | {row['NumFiles/Cols']} | {row.NumRecords} | "
          f"{row.TimeStart} | {row.TimeEnd} |")

print(f"\n報告摘要已儲存至：{OUTPUT_SUMMARY}")
