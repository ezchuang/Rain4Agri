# 資料處理與報告腳本說明 (Data Scripts & Reports)

本文件說明 `data_scripts/` 目錄下的資料處理工具以及根目錄下的 `data_report.py` 的功能與使用方式。

---

## 1. 資料前處理與插補 (`data_scripts/preprocess_and_impute_with_cache.py`)

這是資料處理的核心腳本，負責將原始的 JSON 測站資料轉換為模型可用的 CSV 格式，並處理缺失值。

*   **主要功能**:
    1.  **資料展平 (Flatten)**: 讀取 `data/his_data/<StationID>/*.json`，將巢狀 JSON 轉為平面表格。
    2.  **資料清洗 (Cleaning)**: 自動識別並移除無效值 (如 `-99.5`, `-99.95` 等儀器錯誤代碼)。
    3.  **鄰近站點計算**: 根據經緯度與海拔計算每個測站最近的鄰居，並快取至 `data/web_api/station_neighbors.json`。
    4.  **缺失值插補 (Imputation)**: 使用 **IDW (反距離加權法)**，參考最近的 3 個鄰站數值來填補空缺。支援多核心平行運算 (Multiprocessing)。
*   **輸入**: `data/his_data/` (原始 JSON), `data/web_api/station_list.json` (測站列表)。
*   **輸出**:
    *   `data/cleaned_initial_data.csv`: 初步清洗但未插補的資料。
    *   `data/cleaned_initial_data_imputed.csv`: **(最終模型輸入)** 完整插補後的資料。
    *   `data/logs/preprocess_impute.log`: 處理日誌。

---

## 2. 資料品質報告 (`data_scripts/data_quality_report.py`)

用於分析清洗後資料的品質，偵測異常值與分佈情況。

*   **主要功能**:
    1.  **特殊值統計**: 計算 NaN、空值、以及殘留的錯誤代碼 (-99.5) 的比例。
    2.  **離群值偵測 (Outlier Detection)**: 使用 **IQR (四分位距)** 方法偵測數值異常的極端值。
    3.  **視覺化**: 自動繪製每個欄位的統計圖表。
*   **輸入**: `data/cleaned_initial_data_imputed.csv`
*   **輸出**:
    *   `data/data_quality_summary.csv`: 各欄位的品質統計表。
    *   `data/plots/box_*.png`: 各欄位的箱型圖 (Boxplot)。
    *   `data/plots/hist_*.png`: 各欄位的直方圖 (Histogram)。

---

## 3. 資料管線總結報告 (`data_report.py`)

位於專案根目錄，提供資料處理管線的高層次統計摘要。

*   **主要功能**:
    *   統計資料在「原始 JSON」、「清洗後 CSV」、「插補後 CSV」三個階段的變化。
    *   比較測站數量、資料筆數、時間跨度是否一致。
*   **輸出**:
    *   `data/report_summary.csv`: 管線各階段的統計摘要。
    *   終端機直接印出 Markdown 格式的比較表格。

---

## 4. 相關性分析 (`data_scripts/correlation_analysis_rev.py`)

分析各氣象因子與「累積雨量 (Precipitation_Accumulation)」之間的關聯性。

*   **主要功能**:
    *   計算所有數值欄位與降雨量的 Pearson 相關係數。
    *   繪製相關係數排序圖。
*   **輸出**:
    *   `data/correlation_with_geo/precip_correlation_with_geo.csv`: 相關係數表。
    *   `data/correlation_with_geo/precip_corr_bar_with_geo.png`: 相關係數長條圖。

---

## 建議執行順序

如果您有新的原始資料，請依序執行：

1.  `python data_scripts/preprocess_and_impute_with_cache.py` (產生訓練資料)
2.  `python data_report.py` (確認資料轉換無誤)
3.  `python data_scripts/data_quality_report.py` (檢查資料品質)
