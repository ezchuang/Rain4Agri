# Variables
PYTHON = ./venv/Scripts/python.exe
CHECKPOINT_DIR = checkpoints

.PHONY: help train-implicit train-explicit predict clean

# Default target
help:
	@echo "Available commands:"
	@echo "  make train-implicit        - Train the Implicit model"
	@echo "  make train-explicit        - Train the Explicit model"
	@echo "  make resume-implicit       - Resume Implicit model from interrupted.pth"
	@echo "  make resume-explicit       - Resume Explicit model from interrupted.pth"
	@echo "  make predict               - Run prediction (example)"
	@echo "  make clean                 - Remove pycache files"
	@echo ""
	@echo "You can pass extra arguments using ARGS, e.g.:"
	@echo "  make train-explicit ARGS='--resume checkpoints/my_checkpoint.pth'"

data-report:
	$(PYTHON) -m data_report

train-implicit:
	$(PYTHON) -m rain_model.train_implicit $(ARGS)

train-explicit:
	$(PYTHON) -m rain_model.train_explicit $(ARGS)

resume-implicit:
	$(PYTHON) -m rain_model.train_implicit --resume $(CHECKPOINT_DIR)/interrupted_implicit.pth $(ARGS)

resume-explicit:
	$(PYTHON) -m rain_model.train_explicit --resume $(CHECKPOINT_DIR)/interrupted.pth $(ARGS)

# Example prediction (adjust checkpoint path as needed)
predict:
	$(PYTHON) -m rain_model.predict --model_type explicit --checkpoint $(CHECKPOINT_DIR)/explicit_epoch_50.pth

clean:
	del /s /q __pycache__
	del /s /q *.pyc
