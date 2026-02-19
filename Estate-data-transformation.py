# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.19.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# # CONFIG

import polars as pl
import sqlite3
import tomllib
import glob

from datetime import datetime
import os

import paramiko
from scp import SCPClient

with open("secrets.toml", "rb") as f:
    config = tomllib.load(f)
creds = config["mikrus"]

TABLES = ["offers", "offers_tytan", "offers_gralczyk", "offers_polnoc", "analyzed_offers" ]


# # HELPER FUNCTIONS

def download_db():
    now = datetime.now().strftime("%Y-%m-%d")
    folder_name = "db"
    
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    
    local_filename = f"{now}_olx.db"
    local_full_path = os.path.join(folder_name, local_filename)

    print(f"Connecting {creds['host']}...")
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(
            hostname=creds["host"],
            port=creds["port"],
            username=creds["user"],
            password=creds["password"]
        )
        
        with SCPClient(ssh.get_transport()) as scp:
            print(f"Pobieranie {creds['remote_path']} -> {local_full_path}")
            scp.get(creds["remote_path"], local_full_path)
            
        print(f"Database saved as: {local_full_path}")
        return local_full_path  
        
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        ssh.close()


def get_latest_db_path(folder="db"):
    files = glob.glob(os.path.join(folder, "*.db"))
    
    if not files:
        print("Not found file with extension 'db'!")
        return None
    
    latest_file = max(files)
    print(f"newest file: {latest_file}")
    return latest_file


def load_data_to_df(db_name: str, table_name: str) -> pl.DataFrame | None:
    conn = None
    try:
        conn = sqlite3.connect(db_name)
        print(f"DB connected: {db_name}")

        query = f"SELECT * FROM {table_name}"
        df = pl.read_database(query, conn)

        print(f"DF loaded: {table_name}")
        print(f"DF shape: {df.shape}")
        return df

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        if conn:
            conn.close()
            print("DB connection closed.")


# # DOWNLOADING DATA

download_db()

DB_NAME = get_latest_db_path()


data = {}

for TABLE in TABLES:
    data[TABLE] = load_data_to_df(DB_NAME, TABLE)

data.keys()


