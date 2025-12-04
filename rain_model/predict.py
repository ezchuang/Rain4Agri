
import sys
import os
import torch
import argparse
import pandas as pd
import numpy as np
from torch.utils.data import DataLoader
from tqdm import tqdm

# Add project root to sys.path to allow running as script
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.append(project_root)

from rain_model import config
from rain_model.dataset import RainDataset
from rain_model.arch.implicit import ImplicitSeq2Seq
from rain_model.arch.explicit import ExplicitSeq2Seq

def predict():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model_type', type=str, required=True, choices=['implicit', 'explicit'], help='Model type to use')
    parser.add_argument('--checkpoint', type=str, required=True, help='Path to model checkpoint')
    parser.add_argument('--output', type=str, default='prediction_results.csv', help='Output CSV file')
    args = parser.parse_args()
    
    device = torch.device(config.DEVICE)
    
    # Load Model
    print(f"Loading {args.model_type} model from {args.checkpoint}...")
    if args.model_type == 'implicit':
        model = ImplicitSeq2Seq(config).to(device)
    else:
        model = ExplicitSeq2Seq(config).to(device)
        
    model.load_state_dict(torch.load(args.checkpoint, map_location=device))
    model.eval()
    
    # Data
    # For prediction, we might want to run on the whole dataset or a test split
    # Here we use 'test' mode (which is just full dataset in dataset.py for now, or we can modify)
    # Let's assume we want to predict for the last N days.
    dataset = RainDataset(split='test', log_transform=True)
    loader = DataLoader(dataset, batch_size=1, shuffle=False)
    
    results = []
    
    print("Running prediction...")
    with torch.no_grad():
        for batch in tqdm(loader):
            # input_imgs: (B, Seq, 3, H, W)
            # input_station: (B, Seq, 1, H, W)
            input_imgs, input_station = batch
            
            input_seq = torch.cat([input_imgs, input_station], dim=2).to(device)
            
            # Forward
            # Implicit output: (B, Pred, 1, H, W)
            # Explicit output: (B, Pred, 4, H, W)
            preds = model(input_seq, target_seq=None, teacher_forcing_ratio=0)
            
            # Extract Rain Prediction
            if args.model_type == 'explicit':
                pred_rain = preds[:, :, 3:4] # (B, Pred, 1, H, W)
            else:
                pred_rain = preds # (B, Pred, 1, H, W)
            
            # Inverse Log Transform: exp(x) - 1
            pred_rain = torch.expm1(pred_rain)
                
            # Convert back to Station Values
            # We need to look up station coordinates and extract values
            # This is tricky because batch order matches dataset order, but we need timestamps
            # dataset.timestamps[idx] is the start time?
            # dataset.__getitem__ uses valid_indices.
            # We need to know which timestamp this batch corresponds to.
            # Since batch_size=1 and shuffle=False, we can track index.
            pass 
            # (Simplification: In a real app, we would pass metadata in DataLoader or track indices)
            
            # For now, let's just save the raw grid or a summary
            # Saving full grid for every step is huge.
            # Let's just print shape for verification
            # print(pred_rain.shape)
            
    print("Prediction complete. (Output saving not fully implemented in this demo script)")

if __name__ == "__main__":
    predict()
