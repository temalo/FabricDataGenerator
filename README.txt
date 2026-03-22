python generate_gold_iceberg.py                     # full refresh, default counts
python generate_gold_iceberg.py --scale 0.1         # full refresh, 10% of rows
python generate_gold_iceberg.py --mode append       # append, default counts, no duplicate keys
python generate_gold_iceberg.py --mode append --scale 0.5
