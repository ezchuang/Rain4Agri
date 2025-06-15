#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
名稱：data_quality_report.py

功能：
1. 讀取清洗後資料（CSV），自動偵測以下特殊值：
   - NaN / NULL
   - 空字串 ""
   - -99.5、-9999.5
2. 計算每個欄位上述特殊值的總數與比例。
3. 針對每個數值欄位，利用 IQR 方法偵測離群值（超出 [Q1-1.5*IQR, Q3+1.5*IQR]）。
4. 輸出：
   - CSV 報表：`data_quality_summary.csv`
   - 每個欄位的箱型圖：`plots/box_{column}.png`
   - 每個欄位的直方圖：`plots/hist_{column}.png`
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# ---------- 使用者可調參數 ----------
# 輸入資料路徑
INPUT_CSV = 'data/cleaned_initial_data_imputed.csv'
# 特殊值列表
SPECIAL_VALUES = [np.nan, None, '', -99.5, -9999.5]
# IQR multiplier
IQR_MULTIPLIER = 1.5

# ---------- 主程式 ----------
def main():
    # 1. 讀取資料
    df = pd.read_csv(INPUT_CSV, parse_dates=['DataTime'], dtype=str)
    
    # 2. 先將所有欄位嘗試轉為數字（失敗的保留原字串）
    df_num = df.copy()
    for col in df_num.columns:
        df_num[col] = pd.to_numeric(df_num[col], errors='ignore') # type: ignore
    
    # 3. 建立報表 DataFrame
    report = []
    for col in df.columns:
        series = df[col]
        
        # 特殊值計數
        cnt_nan   = series.isna().sum()
        cnt_empty = (series == '').sum() if series.dtype == object else 0
        cnt_neg   = ((series.astype(float, errors='ignore').isin([-99.5, -9999.5])) 
                     if np.issubdtype(df_num[col].dtype, np.number) else 0)  # type: ignore
        total     = len(series)
        
        # 離群值計算（僅對數值欄位）
        outlier_count = np.nan
        if np.issubdtype(df_num[col].dtype, np.number):  # type: ignore
            s = df_num[col].dropna().astype(float)
            q1, q3 = s.quantile(0.25), s.quantile(0.75)
            iqr    = q3 - q1
            lower, upper = q1 - IQR_MULTIPLIER * iqr, q3 + IQR_MULTIPLIER * iqr
            outlier_count = ((s < lower) | (s > upper)).sum()
        
        report.append({
            'Variable':       col,
            'TotalRows':      total,
            'Count_NaN':      cnt_nan,
            'Pct_NaN':        cnt_nan / total,
            'Count_Empty':    cnt_empty,
            'Pct_Empty':      cnt_empty / total,
            'Count_Place':    cnt_neg if isinstance(cnt_neg, int) else 0,
            'Pct_Place':      (cnt_neg / total) if isinstance(cnt_neg, int) else 0,
            'Count_Outlier':  outlier_count,
            'Pct_Outlier':    (outlier_count / total) if isinstance(outlier_count, (int,float)) else np.nan
        })
    
    df_report = pd.DataFrame(report)
    os.makedirs('plots', exist_ok=True)
    df_report.to_csv('./data/data_quality_summary.csv', index=False, float_format='%.4f')
    print("Report saved to data_quality_summary.csv")
    
    # 4. 繪圖：箱型圖與直方圖
    for col in df_report['Variable']:
        if not np.issubdtype(df_num[col].dtype, np.number):
            continue  # 只畫數值欄位
        
        series = df_num[col].dropna().astype(float)
        if series.empty:
            continue
        
        # 箱型圖
        plt.figure(figsize=(4,6))
        plt.boxplot(series, vert=True)
        plt.title(f'Boxplot of {col}')
        plt.ylabel(col)
        plt.tight_layout()
        plt.savefig(f'plots/box_{col}.png')
        plt.close()
        
        # 直方圖
        plt.figure(figsize=(5,4))
        plt.hist(series, bins=30, edgecolor='black')
        plt.title(f'Histogram of {col}')
        plt.xlabel(col)
        plt.ylabel('Frequency')
        plt.tight_layout()
        plt.savefig(f'./data/plots/hist_{col}.png')
        plt.close()
    
    print("Plots saved under ./plots/")

if __name__ == '__main__':
    main()
