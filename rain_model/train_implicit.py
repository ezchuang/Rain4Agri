
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
from rain_model.arch.implicit import ImplicitSeq2Seq

def masked_mse_loss(pred, target, mask):
    """
    pred: (B, T, 1, H, W)
    target: (B, T, 1, H, W)
    mask: (B, T, 1, H, W) - 1 at station locations, 0 otherwise
    """
    diff = (pred - target) ** 2
    masked_diff = diff * mask
    # Avoid division by zero
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
    print("Initializing Implicit model...")
    model = ImplicitSeq2Seq(config).to(device)
    optimizer = optim.Adam(model.parameters(), lr=config.LEARNING_RATE)
    
    # Resume
    start_epoch = 0
    if args.resume:
        if os.path.isfile(args.resume):
            print(f"Loading checkpoint from {args.resume}")
            checkpoint = torch.load(args.resume)
            model.load_state_dict(checkpoint)
        else:
            print(f"No checkpoint found at {args.resume}")

    # Training Loop
    print(f"Start training for {config.NUM_EPOCHS} epochs...")
    try:
        for epoch in range(start_epoch, config.NUM_EPOCHS):
            model.train()
            total_loss = 0
            
            pbar = tqdm(train_loader, desc=f"Epoch {epoch+1}/{config.NUM_EPOCHS}")
            for batch in pbar:
                # Unpack batch
                # input_imgs: (B, Seq, 3, H, W)
                # input_station: (B, Seq, 1, H, W)
                # target_grids: (B, Pred, 1, H, W)
                # masks: (B, Pred, 1, H, W)
                # target_imgs: (B, Pred, 3, H, W) - Unused in Implicit
                
                input_imgs, input_station, target_grids, masks, _ = batch
                
                # Prepare Input: Concatenate Sat + Rain -> (B, Seq, 4, H, W)
                input_seq = torch.cat([input_imgs, input_station], dim=2).to(device)
                target_grids = target_grids.to(device)
                masks = masks.to(device)
                
                optimizer.zero_grad()
                
                # Forward
                # Output: (B, Pred, 1, H, W)
                preds = model(input_seq, target_grids, teacher_forcing_ratio=config.TEACHER_FORCING_RATIO)
                
                # Loss
                loss = masked_mse_loss(preds, target_grids, masks)
                
                # Backward
                loss.backward()
                optimizer.step()
                
                total_loss += loss.item()
                pbar.set_postfix({'loss': loss.item()})
                
            avg_loss = total_loss / len(train_loader)
            print(f"Epoch {epoch+1} Average Loss: {avg_loss:.6f}")
            
            # Validation Loop
            model.eval()
            val_loss = 0
            with torch.no_grad():
                for batch in val_loader:
                    input_imgs, input_station, target_grids, masks, _ = batch
                    input_seq = torch.cat([input_imgs, input_station], dim=2).to(device)
                    target_grids = target_grids.to(device)
                    masks = masks.to(device)
                    
                    # No teacher forcing during validation
                    preds = model(input_seq, target_seq=None, teacher_forcing_ratio=0)
                    
                    loss = masked_mse_loss(preds, target_grids, masks)
                    val_loss += loss.item()
            
            avg_val_loss = val_loss / len(val_loader) if len(val_loader) > 0 else 0
            print(f"Epoch {epoch+1} Val Loss: {avg_val_loss:.6f}")
            
            # Save Checkpoint
            if (epoch + 1) % 5 == 0:
                path = os.path.join(config.CHECKPOINT_DIR, f"implicit_epoch_{epoch+1}.pth")
                torch.save(model.state_dict(), path)
                print(f"Saved checkpoint to {path}")

    except KeyboardInterrupt:
        print("\nTraining interrupted by user.")
        path = os.path.join(config.CHECKPOINT_DIR, "interrupted_implicit.pth")
        torch.save(model.state_dict(), path)
        print(f"Saved interrupt checkpoint to {path}")
        sys.exit(0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--resume', type=str, default=None, help='Path to checkpoint to resume from')
    args = parser.parse_args()
    train(args)
