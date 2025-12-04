
import os
import json
import pandas as pd
import numpy as np
import torch
from torch.utils.data import Dataset
from PIL import Image
from datetime import datetime, timedelta
import glob
from . import config

class RainDataset(Dataset):
    def __init__(self, split='train', log_transform=True):
        self.split = split
        self.log_transform = log_transform
        self.station_info = self._load_station_info()
        self.df_data = self._load_station_data()
        self.image_paths = self._index_images()
        
        # Get all unique timestamps from station data
        self.timestamps = sorted(self.df_data['DataTime'].unique())
        
        # Split timestamps: 80% Train, 20% Val
        # We split by time to avoid data leakage
        n_total = len(self.timestamps)
        split_idx = int(n_total * 0.8)
        
        # Define valid ranges
        # We need to ensure [idx - SEQ_LEN + 1, idx + PRED_LEN] are within bounds
        # Train: [0, split_idx]
        # Val: [split_idx, end]
        
        if split == 'train':
            # Use DATA_STRIDE to downsample training data
            # End at split_idx - PRED_LEN to ensure targets exist within train set
            # Start at SEQ_LEN
            self.valid_indices = range(config.SEQ_LEN, split_idx - config.PRED_LEN, config.DATA_STRIDE)
        elif split == 'val':
            # Validation: Start after split_idx
            # Ensure we have history (SEQ_LEN) from before split_idx if needed, 
            # but to be safe and clean, let's start at split_idx + SEQ_LEN
            start_idx = split_idx + config.SEQ_LEN
            end_idx = n_total - config.PRED_LEN
            if start_idx < end_idx:
                self.valid_indices = range(start_idx, end_idx, config.DATA_STRIDE) # Can use stride 1 for val if desired
            else:
                self.valid_indices = []
        elif split == 'test' or split == 'all':
            # Test/Inference: Use all possible indices
            self.valid_indices = range(config.SEQ_LEN, n_total)
        else:
            raise ValueError(f"Unknown split: {split}")

    def _load_station_info(self):
        """Load station coordinates from JSON."""
        info = {}
        if not os.path.exists(config.STATION_LIST_PATH):
            print(f"Warning: Station list not found at {config.STATION_LIST_PATH}")
            return info
            
        try:
            with open(config.STATION_LIST_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for group in data.get('data', []):
                    for item in group.get('item', []):
                        sid = item.get('stationID')
                        lat = float(item.get('latitude', 0))
                        lon = float(item.get('longitude', 0))
                        
                        # Map lat/lon to grid coordinates
                        x, y = self._latlon_to_grid(lat, lon)
                        if 0 <= x < config.GRID_SIZE[0] and 0 <= y < config.GRID_SIZE[1]:
                            info[sid] = {'lat': lat, 'lon': lon, 'x': x, 'y': y}
        except Exception as e:
            print(f"Error loading station info: {e}")
            
        return info

    def _latlon_to_grid(self, lat, lon):
        """Map latitude/longitude to grid x/y."""
        # Simple linear mapping
        # x: longitude (0 to W), y: latitude (0 to H)
        # Note: Image usually has (0,0) at top-left. 
        # Map: Lon -> X, Lat -> Y (inverted if needed, but let's keep it simple first)
        
        w, h = config.GRID_SIZE
        x = int((lon - config.MIN_LON) / (config.MAX_LON - config.MIN_LON) * w)
        # Map Lat to Y. Usually higher lat is "up", but image Y is "down".
        # Let's map MaxLat to 0 (top) and MinLat to H (bottom)
        y = int((config.MAX_LAT - lat) / (config.MAX_LAT - config.MIN_LAT) * h)
        return x, y

    def _load_station_data(self):
        """Load and preprocess station CSV data."""
        if not os.path.exists(config.CLEANED_DATA_PATH):
            raise FileNotFoundError(f"Data file not found: {config.CLEANED_DATA_PATH}")
            
        df = pd.read_csv(config.CLEANED_DATA_PATH, parse_dates=['DataTime'])
        # Filter stations that are in our info map (inside BBOX)
        valid_stations = set(self.station_info.keys())
        df = df[df['StationID'].isin(valid_stations)]
        return df

    def _index_images(self):
        """Index available satellite images by timestamp."""
        # Filename format: taiwan_%Y%m%d_%H%M.png
        # We need to map timestamp -> filepath
        # Since images are every 3 hours, we will find the closest one.
        img_map = {}
        if not os.path.exists(config.SATELLITE_DIR):
            return img_map
            
        files = glob.glob(os.path.join(config.SATELLITE_DIR, "*.png"))
        for f in files:
            try:
                basename = os.path.basename(f)
                # Remove prefix and extension
                # taiwan_20250607_1200.png -> 20250607_1200
                ts_str = basename.replace('taiwan_', '').replace('.png', '')
                dt = datetime.strptime(ts_str, "%Y%m%d_%H%M")
                img_map[dt] = f
            except:
                continue
        return img_map

    def _get_image_for_time(self, dt):
        """Find closest image or return zero tensor."""
        # Simple strategy: Round dt to nearest 3 hours
        # Or check exact match first, then search nearby
        # For now, let's try to find exact match or closest within 3 hours
        
        # Round to nearest 3H
        hour = dt.hour
        base_hour = (hour // 3) * 3
        candidate = dt.replace(hour=base_hour, minute=0, second=0, microsecond=0)
        
        path = self.image_paths.get(candidate)
        if not path:
            # Try previous 3H
            prev = candidate - timedelta(hours=3)
            path = self.image_paths.get(prev)
            
        if path:
            try:
                img = Image.open(path).convert('RGB')
                img = img.resize(config.IMG_SIZE)
                # Normalize to [0, 1]
                arr = np.array(img) / 255.0
                return torch.from_numpy(arr).permute(2, 0, 1).float() # C, H, W
            except:
                pass
                
        return torch.zeros(3, *config.IMG_SIZE)

    def __len__(self):
        return len(self.valid_indices)

    def __getitem__(self, idx):
        # Current time index
        curr_idx = self.valid_indices[idx]
        curr_time = self.timestamps[curr_idx]
        
        # 1. Input Sequences (History)
        # [T - SeqLen + 1, ..., T]
        input_imgs = []
        input_station_grids = []
        
        for i in range(config.SEQ_LEN):
            t_idx = curr_idx - config.SEQ_LEN + 1 + i
            t = self.timestamps[t_idx]
            
            # Image
            img = self._get_image_for_time(t)
            input_imgs.append(img)
            
            # Station Grid
            # Get data for this timestamp
            day_data = self.df_data[self.df_data['DataTime'] == t]
            grid = torch.zeros(1, *config.GRID_SIZE) # 1 channel for rain
            
            for _, row in day_data.iterrows():
                sid = row['StationID']
                val = row['Precipitation_Accumulation']
                if sid in self.station_info:
                    x, y = self.station_info[sid]['x'], self.station_info[sid]['y']
                    grid[0, y, x] = val
            
            input_station_grids.append(grid)
            
        # Stack inputs
        # (Seq, C, H, W)
        input_imgs = torch.stack(input_imgs)
        input_station_grids = torch.stack(input_station_grids)
        
        # Apply Log Transform to Inputs
        if self.log_transform:
            input_station_grids = torch.log1p(input_station_grids)
        
        # 2. Target Sequences (Future)
        # [T + 1, ..., T + PredLen]
        target_grids = []
        masks = []
        
        if self.split in ['train', 'val']:
            for i in range(config.PRED_LEN):
                t_idx = curr_idx + 1 + i
                t = self.timestamps[t_idx]
                
                day_data = self.df_data[self.df_data['DataTime'] == t]
                grid = torch.zeros(1, *config.GRID_SIZE)
                mask = torch.zeros(1, *config.GRID_SIZE)
                
                for _, row in day_data.iterrows():
                    sid = row['StationID']
                    val = row['Precipitation_Accumulation']
                    if sid in self.station_info:
                        x, y = self.station_info[sid]['x'], self.station_info[sid]['y']
                        grid[0, y, x] = val
                        mask[0, y, x] = 1.0
                
                target_grids.append(grid)
                masks.append(mask)
            
            target_grids = torch.stack(target_grids)
            masks = torch.stack(masks)
            
            # Apply Log Transform to Targets
            if self.log_transform:
                target_grids = torch.log1p(target_grids)
            
            # Also get future images for Explicit Model (Auxiliary Loss)
            target_imgs = []
            for i in range(config.PRED_LEN):
                t_idx = curr_idx + 1 + i
                t = self.timestamps[t_idx]
                img = self._get_image_for_time(t)
                target_imgs.append(img)
            target_imgs = torch.stack(target_imgs)
            
            return input_imgs, input_station_grids, target_grids, masks, target_imgs
            
        else:
            # Inference mode, no targets
            return input_imgs, input_station_grids
