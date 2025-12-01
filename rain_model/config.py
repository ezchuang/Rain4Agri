
import os
import torch

# --- Paths ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_ROOT = os.path.join(PROJECT_ROOT, 'data')
STATION_LIST_PATH = os.path.join(DATA_ROOT, 'web_api', 'station_list.json')
CLEANED_DATA_PATH = os.path.join(DATA_ROOT, 'cleaned_initial_data_imputed.csv')
SATELLITE_DIR = os.path.join(DATA_ROOT, 'satellite')
CHECKPOINT_DIR = os.path.join(PROJECT_ROOT, 'checkpoints')

# --- Data Parameters ---
IMG_SIZE = (128, 128)  # Resize satellite images to this for training
GRID_SIZE = (128, 128) # Output grid size
SEQ_LEN = 12           # Input sequence length (e.g., past 12 steps)
PRED_LEN = 12          # Prediction sequence length (e.g., future 12 steps)
BATCH_SIZE = 8
NUM_WORKERS = 4

# --- Model Parameters ---
HIDDEN_DIM = 64
KERNEL_SIZE = (3, 3)
NUM_LAYERS = 3

# --- Training Parameters ---
LEARNING_RATE = 1e-4
NUM_EPOCHS = 50
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
TEACHER_FORCING_RATIO = 0.5

# --- Geospatial Bounds (Taiwan) ---
# Used to map lat/lon to grid coordinates
# BBOX: (119.5, 21.5, 122.5, 25.5) from gibs.py
MIN_LON = 119.5
MAX_LON = 122.5
MIN_LAT = 21.5
MAX_LAT = 25.5
