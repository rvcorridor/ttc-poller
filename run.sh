mkdir pbufs >> err.txt
mkdir raw >> err.txt

./venv/bin/python3 main.py
sleep 15
./venv/bin/python3 main.py
sleep 15
./venv/bin/python3 main.py
sleep 15
./venv/bin/python3 main.py

