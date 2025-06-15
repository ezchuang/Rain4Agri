import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# 1. 定位 Geo-Filtered 資料
candidates = [
    'cleaned_initial_data_imputed.csv',
    './data/cleaned_initial_data_imputed.csv'
]
csv_path = next((p for p in candidates if os.path.exists(p)), None)
if not csv_path:
    raise FileNotFoundError(f"找不到 cleaned_meteo_data_with_geo.csv，請放在以下任一路徑之一：{candidates}")

# 2. 讀取並預處理
df = pd.read_csv(csv_path, parse_dates=['DataTime'])
df_num = df.select_dtypes(include=[np.number])
df_num = df_num.dropna(axis=1, thresh=int(len(df_num)*0.5))
df_num = df_num.fillna(df_num.mean())

# 3. 計算 Precipitation_Accumulation 的相關係數
prec_corr = df_num.corr()['Precipitation_Accumulation'].sort_values(ascending=False).reset_index()
prec_corr.columns = ['Variable', 'Correlation']

# 4. 輸出 CSV
out_dir = './data/correlation_with_geo'
os.makedirs(out_dir, exist_ok=True)
prec_corr.to_csv(os.path.join(out_dir, 'precip_correlation_with_geo.csv'), index=False)

# 5. 以純 Python 生成 Markdown 表格 (無需 tabulate)
cols = prec_corr.columns.tolist()
# 表頭
header = "| " + " | ".join(cols) + " |"
sep = "| " + " | ".join(["---"] * len(cols)) + " |"
lines = [header, sep]
# 內容
for _, row in prec_corr.iterrows():
    line = "| " + " | ".join(
        f"{row[col]:.4f}" if isinstance(row[col], (float, np.floating)) else str(row[col])
        for col in cols
    ) + " |"
    lines.append(line)

print("\n".join(lines))

# 6. 繪製水平長條圖
plt.figure(figsize=(8, 5))
plt.barh(prec_corr['Variable'], prec_corr['Correlation'])
plt.xlabel('Correlation with Precipitation_Accumulation')
plt.title('Correlation after Geo-Filtering')
plt.tight_layout()
out_png = os.path.join(out_dir, 'precip_corr_bar_with_geo.png')
plt.savefig(out_png, dpi=300)
plt.show()

print(f"\nCSV saved to {out_dir}/precip_correlation_with_geo.csv")
print(f"Bar chart saved to {out_dir}/precip_corr_bar_with_geo.png")
