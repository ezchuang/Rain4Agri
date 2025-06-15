import os
import pandas as pd
import matplotlib.pyplot as plt

# ---------------- CONFIGURATION ----------------
# CSV file containing Variable and Correlation columns
candidates = [
    'precip_correlation_with_geo.csv',
    './data/correlation_with_geo/precip_correlation_with_geo.csv'
]
input_csv = next((p for p in candidates if os.path.exists(p)), None)
if not input_csv:
    raise FileNotFoundError(f"找不到相關係數檔案，請確認範本 CSV 存在於 {candidates} 其中之一")

# Threshold for absolute correlation: variables below this will be removed
threshold = 0.044

# ---------------- READ DATA ----------------
df = pd.read_csv(input_csv)

# Exclude the target variable itself
df = df[df['Variable'] != 'Precipitation_Accumulation'].copy()

# Compute absolute correlation for filtering and sorting
df['AbsCorr'] = df['Correlation'].abs()

# Sort variables by absolute correlation (ascending)
df_sorted = df.sort_values('AbsCorr')

# Identify low-correlation variables to remove
to_remove = df_sorted[df_sorted['AbsCorr'] < threshold]['Variable'].tolist()

# Identify high-correlation variables to keep
to_keep = df_sorted[df_sorted['AbsCorr'] >= threshold]['Variable'].tolist()

# ---------------- OUTPUT RESULTS ----------------
out_dir = os.path.dirname(input_csv) or '.'
# Save sorted list
df_sorted.drop(columns='AbsCorr').to_csv(os.path.join(out_dir, 'corr_sorted.csv'), index=False)
# Save removal list
pd.DataFrame({'Variable': to_remove}).to_csv(os.path.join(out_dir, 'low_corr_variables.csv'), index=False)
# Save keep list
pd.DataFrame({'Variable': to_keep}).to_csv(os.path.join(out_dir, 'high_corr_variables.csv'), index=False)

# Print sorted table
print("=== Variables sorted by |Correlation| (ascending) ===")
print(df_sorted[['Variable', 'Correlation']].to_string(index=False))

# Print lists
print(f"\nVariables with |Correlation| < {threshold} (to remove):")
print(to_remove)
print(f"\nVariables with |Correlation| >= {threshold} (to keep):")
print(to_keep)

# ---------------- PLOT ----------------
plt.figure(figsize=(8, 6))
plt.barh(df_sorted['Variable'], df_sorted['Correlation'])
plt.axvline(threshold, color='red', linestyle='--', label=f'Threshold = {threshold}')
plt.axvline(-threshold, color='red', linestyle='--')
plt.xlabel('Correlation with Precipitation_Accumulation')
plt.title('Correlation Sorted & Threshold Filtering')
plt.legend()
plt.tight_layout()
plot_path = os.path.join(out_dir, 'corr_filter_plot.png')
plt.savefig(plot_path, dpi=300)
plt.show()

print(f"\nSorted correlations saved to {out_dir}/corr_sorted.csv")
print(f"Low-corr variables saved to {out_dir}/low_corr_variables.csv")
print(f"High-corr variables saved to {out_dir}/high_corr_variables.csv")
print(f"Filter plot saved to {plot_path}")
