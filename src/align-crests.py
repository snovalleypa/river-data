import datetime
from datetime import timedelta
import pandas as pd
import numpy as np
import os

from pandas.core.frame import DataFrame

def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

def find_crest(timestamp: datetime, df: DataFrame, upstream: bool):
  if upstream:
    down_rows = df[(df['utc'] < timestamp) & (df['utc'] > timestamp - timedelta(hours=24) )]
  else:
    down_rows = df[(df['utc'] > timestamp) & (df['utc'] < timestamp + timedelta(hours=24) )]
  if down_rows.__len__() > 0:
    found_row = down_rows.iloc[0]
    print(timestamp, found_row['utc'], (found_row['utc'] - timestamp).total_seconds() / (60.0 * 60.0))
    return found_row
  return None

def align_crests(updstream_crests_path:str, downstread_crests_path:str):
  aligned_crests = []
  up_df = pd.read_csv(updstream_crests_path, parse_dates=['utc'], delimiter="\t")
  down_df = pd.read_csv(downstread_crests_path, parse_dates=['utc'], delimiter="\t")
  for label, up_row in up_df.iterrows():
    found_row = find_crest(up_row['utc'], down_df, False)
    if None != found_row:
      aligned_crests.append({
          'up_utc': up_row['utc'],
          'down_utc': found_row['utc'],
          'crest_hours': (found_row['utc'] - up_row['utc']).total_seconds() / (60.0 * 60.0),
          'up_discharge': up_row['discharge'],
          'down_discharge': found_row['discharge'],
        })
  aligned_df = pd.DataFrame(aligned_crests)
  path = 'crests/aligned.csv'
  aligned_df.to_csv(path, sep="\t", date_format="%Y-%m-%d %H:%M:%S")

def main():
  # Falls => # Carnation
  align_crests('crests/usgs/forks/usgs-forks-crests.csv', 'crests/usgs/12144500/usgs-12144500-crests.csv')

main()