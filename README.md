# Rain4Agri - 農業降雨預測系統

本專案旨在結合 **衛星雲圖 (空間特徵)** 與 **地面測站數據 (時序數值)**，利用深度學習模型預測未來的降雨分佈與降雨量，以輔助農業決策。

## 📁 專案結構

*   **`crawler/`**: 資料爬蟲腳本
    *   `cwa_his_data_crawler_rev2.py`: 抓取 CWA 歷史氣象站數據
    *   `gibs.py`: 抓取 NASA GIBS 衛星雲圖
*   **`data_scripts/`**: 資料前處理與清洗腳本
    *   `preprocess_and_impute_with_cache.py`: 負責資料清洗與缺失值插補
    *   `data_report.py`: 產生資料統計報告
*   **`rain_model/`**: **(核心)** 新版深度學習模型實作
    *   `arch/`: 模型架構 (Implicit Seq2Seq, Explicit Seq2Seq)
    *   `config.py`: 全域參數設定 (路徑、超參數、資料取樣設定)
    *   `dataset.py`: 資料讀取器 (整合 CSV 與 衛星圖)
    *   `train_implicit.py`: 訓練腳本 (隱式遞迴方案)
    *   `train_explicit.py`: 訓練腳本 (顯式遞迴方案)
    *   `predict.py`: 推論/預測腳本
*   **`model/`**: (舊版) 僅基於數值的 LSTM 模型實驗

## 🚀 環境建置

請確保已安裝 Python 3.8+，並安裝必要套件：

```bash
pip install -r requirements.txt
```

主要依賴：`torch` (建議搭配 CUDA), `pandas`, `numpy`, `Pillow`, `tqdm`.

## 📊 資料準備

模型訓練需要兩類資料：
1.  **測站資料**: 位於 `data/cleaned_initial_data_imputed.csv` (由 `data_scripts` 產出)
2.  **衛星雲圖**: 位於 `data/satellite/` (由 `crawler/gibs.py` 下載)

## 🧠 模型訓練

本專案提供兩種模型架構，皆位於 `rain_model` 套件中。

### 1. 隱式遞迴模型 (Implicit Model)
*   **特點**: 專注於降雨預測，Decoder 僅傳遞 Hidden State。
*   **適用**: 追求降雨數值準確度，訓練速度較快。
*   **執行指令**:
    ```bash
    python -m rain_model.train_implicit
    ```

### 2. 顯式/混合模型 (Explicit Model)
*   **特點**: 同時預測「未來降雨」與「未來衛星雲圖」。
*   **適用**: 需要可解釋性 (可觀察模型預測的雲層移動)，作為 Regularization 輔助訓練。
*   **執行指令**:
    ```bash
    python -m rain_model.train_explicit
    ```

### ⚙️ 參數調整 (`rain_model/config.py`)
您可以在 `config.py` 中調整所有參數。
*   **效能優化**: 目前設定 `DATA_STRIDE = 6` (每 6 小時取樣一次) 以加快訓練速度。若需更多資料可調小此值。
*   **Windows 相容性**: 設定 `NUM_WORKERS = 0` 以避免 Windows 下的多工錯誤。

## 🔮 預測 (Inference)

訓練完成後，模型權重會儲存在 `checkpoints/` 目錄下。使用以下指令進行預測：

```bash
# 範例：使用 Explicit 模型，載入第 50 epoch 的權重
python -m rain_model.predict --model_type explicit --checkpoint checkpoints/explicit_epoch_50.pth
```

## ⚠️ 已知問題

*   **Windows Multiprocessing**: 在 Windows 環境下，PyTorch DataLoader 若設定 `num_workers > 0` 可能會因資料量大導致 Pickling Error。目前的 `config.py` 已預設 `NUM_WORKERS = 0` 來解決此問題。
