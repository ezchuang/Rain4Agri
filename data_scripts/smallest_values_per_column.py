import os
import pandas as pd
import numpy as np

# ---------- 配置 ----------
INPUT_CSV = 'data/cleaned_initial_data_imputed.csv'
OUTPUT_CSV = 'data/smallest_values_per_column.csv'
N_SMALL = 5  # 取最小的 N 個值

# ---------- 讀取與計算 ----------
if not os.path.exists(INPUT_CSV):
    raise FileNotFoundError(f"找不到檔案：{INPUT_CSV}")

df = pd.read_csv(INPUT_CSV, parse_dates=['DataTime'])

# 擷取數值欄位
num_cols = df.select_dtypes(include=[np.number]).columns.tolist()

# 計算每個欄位最小的 N_SMALL 個唯一路徑
records = []
for col in num_cols:
    vals = df[col].dropna().astype(float)
    if vals.empty:
        continue
    # 取得前 2*N_SMALL 最小值，去重後取前 N_SMALL
    smallest = vals.nsmallest(N_SMALL * 2).unique()[:N_SMALL]
    records.append({
        'Variable': col,
        'SmallestValues': "; ".join(f"{v:.4f}" for v in smallest)
    })

out_df = pd.DataFrame(records)

# ---------- 輸出結果 ----------
os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
out_df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')

# 列印 Markdown 表格
print("| Variable | Smallest Values |")
print("|---|---|")
for _, row in out_df.iterrows():
    print(f"| {row['Variable']} | {row['SmallestValues']} |")

print(f"\n已將結果儲存至：{OUTPUT_CSV}")
