cd "$HOME/ttc-poller" || exit 1

export DATA_DIR="datav0.1"

source "venv/bin/activate"
python3 "main.py"

