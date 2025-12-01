
import torch
import torch.nn as nn
import random
from .layers import ConvLSTMCell

class ExplicitSeq2Seq(nn.Module):
    def __init__(self, config):
        super(ExplicitSeq2Seq, self).__init__()
        self.config = config
        
        # Encoder: Takes Sat(3) + Rain(1) = 4 channels
        self.enc_input_dim = 4
        self.hidden_dim = config.HIDDEN_DIM
        self.kernel_size = config.KERNEL_SIZE
        self.num_layers = config.NUM_LAYERS
        
        self.encoder_cells = nn.ModuleList()
        for i in range(self.num_layers):
            in_dim = self.enc_input_dim if i == 0 else self.hidden_dim
            self.encoder_cells.append(ConvLSTMCell(in_dim, self.hidden_dim, self.kernel_size))
            
        # Decoder: Takes Sat(3) + Rain(1) = 4 channels (Explicit Recurrence)
        self.dec_input_dim = 4
        self.decoder_cells = nn.ModuleList()
        for i in range(self.num_layers):
            in_dim = self.dec_input_dim if i == 0 else self.hidden_dim
            self.decoder_cells.append(ConvLSTMCell(in_dim, self.hidden_dim, self.kernel_size))
            
        # Output Layer: Hidden -> Sat(3) + Rain(1)
        self.out_conv = nn.Conv2d(self.hidden_dim, 4, kernel_size=1)
        
    def forward(self, input_seq, target_seq=None, teacher_forcing_ratio=0.5):
        # input_seq: (B, Seq, 4, H, W)
        # target_seq: (B, Pred, 4, H, W) - Contains both Rain and Sat targets
        
        batch_size, seq_len, _, h, w = input_seq.size()
        
        # Initialize hidden states
        hidden_states = [cell.init_hidden(batch_size, (h, w)) for cell in self.encoder_cells]
        
        # --- Encoder ---
        for t in range(seq_len):
            input_t = input_seq[:, t]
            for i, cell in enumerate(self.encoder_cells):
                inp = input_t if i == 0 else hidden_states[i-1][0]
                hidden_states[i] = cell(inp, hidden_states[i])
                
        # --- Decoder ---
        # Initial input: Last Frame from Encoder input
        dec_input = input_seq[:, -1] # (B, 4, H, W)
        
        outputs = []
        pred_len = self.config.PRED_LEN
        
        for t in range(pred_len):
            for i, cell in enumerate(self.decoder_cells):
                inp = dec_input if i == 0 else hidden_states[i-1][0]
                hidden_states[i] = cell(inp, hidden_states[i])
            
            # Output
            last_hidden = hidden_states[-1][0]
            pred = self.out_conv(last_hidden) # (B, 4, H, W)
            outputs.append(pred)
            
            # Determine next input
            use_teacher_forcing = False
            if target_seq is not None:
                use_teacher_forcing = random.random() < teacher_forcing_ratio
            
            if use_teacher_forcing:
                dec_input = target_seq[:, t]
            else:
                dec_input = pred
                
        outputs = torch.stack(outputs, dim=1) # (B, Pred, 4, H, W)
        return outputs
