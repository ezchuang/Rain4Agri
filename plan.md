# Model Implementation Plan: Satellite CNN + Station Data Fusion

## 1. 核心概念 (Core Concept)
結合 **衛星雲圖 (空間特徵)** 與 **測站數據 (時序/精確數值)**，預測未來的降雨機率與降雨量。
目標是產生具備 **空間連續性** 的棋盤式 (Grid) 預測結果，並利用測站位置的真實數據進行驗證與校正。

## 2. 架構設計 (Architecture Design)

### A. 輸入資料 (Inputs)
1.  **衛星雲圖 (Satellite Images)** - *Grid Data*
    *   **處理方式**: 使用 **CNN (卷積神經網路)** 提取空間特徵 (如雲層厚度、移動趨勢)。
    *   **目的**: 提供廣域的降雨潛勢背景。
2.  **測站數據 (Station Data)** - *Point Data*
    *   **處理方式**: 使用 **LSTM/GRU** 處理時序變化。
    *   **目的**: 提供特定座標的精確歷史降雨資訊。

### B. "棋盤式" 輸出與空間連續性 (The "Chessboard" & Continuity)
*   **模型輸出**:
    *   模型將輸出一個 **2D 網格圖 (Grid Map/Heatmap)**，每個網格點代表該區域的降雨預測值。
*   **空間連續性 (Spatial Continuity)**:
    *   使用 **Transposed Convolution (反卷積)** 或 **Upsampling** 層來生成輸出，確保數值在空間上平滑過渡，避免相鄰網格數值跳躍。
    *   可加入 **Total Variation Loss (全變分損失)** 作為訓練限制，強制模型保持輸出平滑。
*   **驗證機制 (Verification)**:
    *   雖然輸出是整張網格圖，但在計算 Loss (誤差) 時，我們只取 **測站所在座標 (Location-based)** 的預測值與真實值進行比對。
    *   `Loss = Error(Grid_Prediction[Station_Loc], Real_Station_Value)`

### C. 融合與預測流程 (Fusion & Prediction Flow)
1.  **CNN Encoder**: 衛星圖 $I_t$ $\rightarrow$ 空間特徵圖 $F_{spatial}$。
2.  **Data Embedding**: 將測站數據映射到與特徵圖相同大小的網格上 (Sparse Grid)。
3.  **Fusion Layer**: 結合 $F_{spatial}$ 與測站特徵 (例如使用 Concatenation 或 Attention 機制)。
4.  **Decoder**: 解碼為預測網格 (Next Step Prediction)。
    *   Output 1: **降雨機率圖 (Probability Map)**
    *   Output 2: **降雨量圖 (Amount Map)**

## 3. 遞迴預測與校正 (Recursive Prediction & Correction)

### A. 遞迴預測 (Recursive Strategy) - 兩大流派比較
針對您的提議 (預測「處理後的圖形結構」並當作下一次輸入)，這在深度學習中稱為 **「顯式特徵遞迴 (Explicit Feature Recurrence)」**，與我原本提的 **「隱式狀態遞迴 (Implicit Hidden State)」** 各有優劣。

#### 方案一：隱式遞迴 (原本建議 - Seq2Seq/ConvLSTM)
*   **機制**: Decoder 只傳遞 `Hidden State` (黑盒子記憶)，不強制輸出未來的衛星特徵。
*   **優點**: 模型專注於「降雨」準確度。`Hidden State` 可以包含人類看不懂但對預測有用的高維資訊。
*   **缺點**: 無法直觀看到模型「想像」中的未來雲圖長什麼樣。

#### 方案二：顯式遞迴 (您的提議 - Next Frame Prediction)
*   **機制**: 模型同時預測 `[未來降雨, 未來衛星特徵]`，並將預測出的特徵餵給下一步。
*   **優點**:
    1.  **可解釋性高**: 您可以看到模型預測的雲層移動是否合理 (如：雲往東移，雨區跟著移)。
    2.  **物理約束**: 強迫模型學會雲層的物理運動規律，而不僅僅是數值擬合。
*   **缺點**:
    1.  **模糊化 (Blurriness)**: 預測未來的特徵圖容易隨時間變得模糊，導致長期預測 (一天後) 精度下降。
    2.  **訓練難度高**: 模型要同時顧好「畫圖」和「算雨量」，負擔較重。

#### 建議策略 (Hybrid Approach)
**「都做做看」是正確的，但我們可以設計一個模型同時支援兩者！**
*   **主架構**: 採用 Seq2Seq。
*   **訓練時**: 加入 **Auxiliary Loss (輔助損失)**，要求模型在輸出降雨的同時，順便重建下一刻的特徵圖。
    *   `Total_Loss = Loss_Rain + 0.5 * Loss_Feature_Reconstruction`
*   **預測時**: 可以選擇只用 Hidden State (方案一)，或者混合使用預測出的特徵 (方案二)。
*   **結論**: 您的提議能作為強大的 **Regularizer (正規化項)**，幫助模型學得更好，我們應該將其納入設計。

### B. 校正 (Correction)
*   **模型校正**: 在每個時間步，如果有新的真實測站數據進來，可以用來更新模型的隱藏狀態 (Hidden State) 或進行 Online Learning。
*   **後處理校正**: 針對長期預測 (Day/Week)，建立非 ML 的統計模型來分析預測偏差，並對輸出結果進行加權修正。

## 4. 視覺化與呈現 (Visualization)
*   **分佈圖 (Distribution Map)**: 直接將模型輸出的 Grid Map 渲染為熱力圖 (Heatmap) 供前端顯示。
*   **統計圖表 (Statistical Charts)**:
    *   針對特定測站，繪製「預測 vs 真實」的時間序列圖。
    *   計算準確度指標 (RMSE, Accuracy) 並繪製隨預測時間長度變化的趨勢圖。

## 5. 執行步驟 (Action Plan)
1.  **資料對齊 (Data Alignment)**: 確保衛星圖與測站資料的時間戳記一致。
2.  **模型建置 (Model Building)**: 實作 CNN-LSTM Fusion 架構。
3.  **訓練 (Training)**: 定義 Loss Function (MSE + BCE + Smoothness) 並開始訓練。
4.  **預測腳本 (Inference Script)**: 實作遞迴預測邏輯，並產出 JSON/CSV 供前端使用。
