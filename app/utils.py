import os

def make_dir(path: str):
    os.makedirs(path, exist_ok=True)