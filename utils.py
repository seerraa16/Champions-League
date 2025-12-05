# utils.py
import os
import pandas as pd
import glob
import json
from unidecode import unidecode
from fuzzywuzzy import process

def list_csv(folder):
    return sorted(glob.glob(os.path.join(folder, "*.csv")))

def read_csv_safe(path):
    try:
        return pd.read_csv(path, encoding="utf-8")
    except:
        return pd.read_csv(path, encoding="latin1")

def normalize_name(s):
    if pd.isna(s): return ""
    s2 = unidecode(str(s)).lower().strip()
    s2 = s2.replace("fc ", "").replace("cf ", "").replace("ac ", "")
    return s2

def save_jsonl(path, records):
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
