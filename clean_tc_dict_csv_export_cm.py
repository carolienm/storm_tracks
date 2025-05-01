import csv
import datetime
import pandas as pd

##############################################################################33
#This code was originally written by Max Shumpai
#With the instructions 
# 1. Download csv for region of choice from NCEI NOAA 
#https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r01/access/csv/#
#	- They come in this format: ibtracs.{REGION}.list.v0{VERSION}.csv
#	- Move the csv to the current workspace folder.  #
#Edited and updated by Carolien Mossel in April 2025#
#to handle different date formats and output a csv instead of .txt file#
###############################################################################

# Config
REGION = "NA"
VERSION = "4r01"
csv_file = f"ibtracs.{REGION}.list.v0{VERSION}.csv"
columns_to_extract = ["SID", "ISO_TIME", "LAT", "LON", "USA_STATUS","USA_WIND","USA_PRES"]

# Helper function to parse date with multiple formats
def parse_date(date_str):
    formats = [
        '%Y-%m-%d %H:%M:%S',      # e.g. '2024-11-17 15:00:00'
        '%m/%d/%Y %I:%M:%S %p',   # e.g. '11/17/2024  3:00:00 PM'
        '%m/%d/%Y %H:%M'          # e.g. '9/30/2024 6:00'
    ]
    for fmt in formats:
        try:
            return datetime.datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    raise ValueError(f"Time data '{date_str}' does not match known formats.")

# Read CSV into list of dicts
rows = []
with open(csv_file, 'r') as file:
    reader = csv.DictReader(file)
    next(reader)  # Skip second row
    for row in reader:
        try:
            row_data = {
                'SID': row['SID'],
                'ISO_TIME': parse_date(row['ISO_TIME']),
                'LAT': float(row['LAT']),
                'LON': float(row['LON']),
                'STAT': row['USA_STATUS'],
                'WIND': row['USA_WIND'],
                'SLP': row['USA_PRES'],
            }
            rows.append(row_data)
        except Exception as e:
            print(f"Skipping row due to error: {e}")

# Convert to DataFrame
df = pd.DataFrame(rows)

# Filter to 6-hourly timestamps
df = df[df['ISO_TIME'].dt.hour.isin([0, 6, 12, 18])]

# Assign numeric storm IDs
df['storm_id'] = df.groupby('SID').ngroup() + 1

# Sort to ensure proper time order
df = df.sort_values(by=['storm_id', 'ISO_TIME']).reset_index(drop=True)

# Time difference validation per storm
for storm_id, group in df.groupby('storm_id'):
    time_diffs = group['ISO_TIME'].diff().dropna()
    irregular = time_diffs[time_diffs != datetime.timedelta(hours=6)]
    if not irregular.empty:
        for idx, diff in irregular.items():
            print(f"[Warning] Storm {storm_id} has irregular time_diff at index {idx}: {diff} (expected 6 hours)")

# Save the cleaned, filtered data
df.to_csv(f"ibtracs.{REGION}.list.v0{VERSION}.processed_6hrly.statslp3.csv", index=False)
# Optionally: df.to_pickle(...) or df.to_parquet(...)
