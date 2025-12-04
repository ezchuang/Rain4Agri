
import sys
import os
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader
import argparse
from tqdm import tqdm

# Add project root to sys.path to allow running as script
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.append(project_root)

from rain_model import config
from rain_model.dataset import RainDataset
from rain_model.arch.explicit import ExplicitSeq2Seq

def masked_mse_loss(pred, target, mask):
    """
    pred: (B, T, 1, H, W)
    target: (B, T, 1, H, W)
    mask: (B, T, 1, H, W)
    """
    diff = (pred - target) ** 2
    masked_diff = diff * mask
    loss = masked_diff.sum() / (mask.sum() + 1e-6)
    return loss

def train(args):
    # Setup
    os.makedirs(config.CHECKPOINT_DIR, exist_ok=True)
    device = torch.device(config.DEVICE)
    
    # Data
    print("Loading dataset...")
    train_dataset = RainDataset(split='train')
    train_loader = DataLoader(train_dataset, batch_size=config.BATCH_SIZE, shuffle=True, num_workers=config.NUM_WORKERS)
    
    val_dataset = RainDataset(split='val')
    val_loader = DataLoader(val_dataset, batch_size=config.BATCH_SIZE, shuffle=False, num_workers=config.NUM_WORKERS)
    
    # Model
    print("Initializing Explicit model...")
    model = ExplicitSeq2Seq(config).to(device)
    optimizer = optim.Adam(model.parameters(), lr=config.LEARNING_RATE)
    criterion_sat = nn.MSELoss()

    # Resume
    start_epoch = 0
    if args.resume:
        if os.path.isfile(args.resume):
            print(f"Loading checkpoint from {args.resume}")
            checkpoint = torch.load(args.resume)
            model.load_state_dict(checkpoint)
            # If we saved optimizer state, we could load it too. 
            # For now, simple model weight resume.
            # Ideally, we should save/load optimizer and epoch.
            # Let's assume user manages epoch manually or we infer from filename?
            # Or better, let's just start from 0 if not saved, or try to parse filename.
            # But simpler: just load weights.
        else:
            print(f"No checkpoint found at {args.resume}")

    # Training Loop
    print(f"Start training for {config.NUM_EPOCHS} epochs...")
    try:
        for epoch in range(start_epoch, config.NUM_EPOCHS):
            model.train()
            total_loss = 0
            total_rain_loss = 0
            total_sat_loss = 0
            
            pbar = tqdm(train_loader, desc=f"Epoch {epoch+1}/{config.NUM_EPOCHS}")
            for batch in pbar:
                # Unpack batch
                input_imgs, input_station, target_grids, masks, target_imgs = batch
                
                # Prepare Input: Concatenate Sat(3) + Rain(1) -> (B, Seq, 4, H, W)
                input_seq = torch.cat([input_imgs, input_station], dim=2).to(device)
                
                # Prepare Target: Concatenate Sat(3) + Rain(1) -> (B, Pred, 4, H, W)
                # Note: Explicit model outputs [Sat(3), Rain(1)] or [Rain(1), Sat(3)]?
                # In ExplicitSeq2Seq, out_conv is 4 channels.
                # Let's assume output order is same as input: Sat(3) then Rain(1).
                # But wait, in dataset we stack them.
                # Let's align: Output Channel 0-2: Sat, Channel 3: Rain.
                
                target_sat = target_imgs.to(device)
                target_rain = target_grids.to(device)
                masks = masks.to(device)
                
                # Combine targets for teacher forcing if needed, but we calculate loss separately
                # For teacher forcing input, we need (B, Pred, 4, H, W)
                target_combined = torch.cat([target_sat, target_rain], dim=2)
                
                optimizer.zero_grad()
                
                # Forward
                # Output: (B, Pred, 4, H, W)
                preds = model(input_seq, target_combined, teacher_forcing_ratio=config.TEACHER_FORCING_RATIO)
                
                # Split Output
                pred_sat = preds[:, :, :3]
                pred_rain = preds[:, :, 3:4]
                
                # Loss
                loss_rain = masked_mse_loss(pred_rain, target_rain, masks)
                loss_sat = criterion_sat(pred_sat, target_sat)
                
                # Total Loss (Hybrid)
                # Weighting: Rain is primary, Sat is auxiliary (0.5)
                loss = loss_rain + 0.5 * loss_sat
                
                # Backward
                loss.backward()
                optimizer.step()
                
                total_loss += loss.item()
                total_rain_loss += loss_rain.item()
                total_sat_loss += loss_sat.item()
                
                pbar.set_postfix({'loss': loss.item(), 'rain': loss_rain.item(), 'sat': loss_sat.item()})
                
            avg_loss = total_loss / len(train_loader)
            print(f"Epoch {epoch+1} Avg Loss: {avg_loss:.6f} (Rain: {total_rain_loss/len(train_loader):.6f}, Sat: {total_sat_loss/len(train_loader):.6f})")
            
            # Validation Loop
            model.eval()
            val_loss = 0
            with torch.no_grad():
                for batch in val_loader:
                    input_imgs, input_station, target_grids, masks, target_imgs = batch
                    input_seq = torch.cat([input_imgs, input_station], dim=2).to(device)
                    target_sat = target_imgs.to(device)
                    target_rain = target_grids.to(device)
                    masks = masks.to(device)
                    
                    # No teacher forcing during validation
                    preds = model(input_seq, target_seq=None, teacher_forcing_ratio=0)
                    
                    pred_sat = preds[:, :, :3]
                    pred_rain = preds[:, :, 3:4]
                    
                    loss_rain = masked_mse_loss(pred_rain, target_rain, masks)
                    loss_sat = criterion_sat(pred_sat, target_sat)
                    loss = loss_rain + 0.5 * loss_sat
                    val_loss += loss.item()
            
            avg_val_loss = val_loss / len(val_loader) if len(val_loader) > 0 else 0
            print(f"Epoch {epoch+1} Val Loss: {avg_val_loss:.6f}")
            
            # Save Checkpoint
            if (epoch + 1) % 5 == 0:
                path = os.path.join(config.CHECKPOINT_DIR, f"explicit_epoch_{epoch+1}.pth")
                torch.save(model.state_dict(), path)
                print(f"Saved checkpoint to {path}")

    except KeyboardInterrupt:
        print("\nTraining interrupted by user.")
        path = os.path.join(config.CHECKPOINT_DIR, "interrupted.pth")
        torch.save(model.state_dict(), path)
        print(f"Saved interrupt checkpoint to {path}")
        sys.exit(0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--resume', type=str, default=None, help='Path to checkpoint to resume from')
    args = parser.parse_args()
    train(args)
