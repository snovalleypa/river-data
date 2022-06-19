#!/usr/bin/env python3
import datetime
from datetime import timedelta
import pandas as pd
import numpy as np
import json
import os

def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

def calc_crest(path:str):
  df = pd.read_parquet(path)
  #df = pd.read_parquet('data/usgs/')
  df.sort_index(inplace=True)
  crests = []

  #identify crests
  count = 0
  previous_min = np.inf
  previous_min_at = datetime.date(1920, 1, 1)
  previous_max = 0
  previous_max_min = np.inf # min value when max read
  previous_max_at = datetime.date(1920, 1, 1)
  max_row = None
  
  #time gap
  gap_hours = 18
  crests = []
  min_delta_ratio = 0.25 # 25%
  measure = 'stage'
  stage_delta = 1.0 # ft


  for timestamp, row in df.iterrows():
      count += 1
      discharge = row[measure]
      if discharge == None or np.isnan(discharge):
        continue
  #    print(timestamp, discharge)
      if discharge <= previous_min:
          previous_min = discharge
          previous_min_at = timestamp
          previous_max = discharge
          previous_max_at = timestamp
          continue
          
      if discharge > previous_max:
          previous_max = discharge
          previous_max_min = previous_min
          previous_max_at = timestamp
          max_row = row
          continue
          
      if (timestamp - previous_max_at) < timedelta(hours=gap_hours):
          continue
          
      if (measure == 'discharge' and previous_max - previous_min < (min_delta_ratio * previous_max) ) or (previous_max - previous_max_min < stage_delta):
          continue
          
      print('CREST:', path, previous_max_at, previous_max)
      crests.append(max_row)
      
      previous_min = discharge
      previous_min_at = timestamp
      previous_max = 0
      previous_max_at = 0
      max_row = None
      # if count > 5000:
      #   break

  return pd.DataFrame(crests)

def gen_crests(station_path:str):
  df_crests = calc_crest('data/' + station_path)
  path = 'crests/' + station_path + "/{}-crests.csv".format(station_path.replace("/","-"))
  ensure_dir(path)
  df_crests.to_csv(path,index_label="utc", sep="\t", date_format="%Y-%m-%d %H:%M:%S")

def main():
  with open('locations.json') as json_file:
    data = json.load(json_file)
    locations = data['locations']
    for loc in locations:
      station_path = '{}/{}'.format(loc['source'].lower(), loc['id'])
      gen_crests(station_path)


  # gen_crests('usgs/12149000') # Carnation
#   gen_crests('usgs/12144500') # Falls
#   gen_crests('usgs/12141300') # MF
# gen_crests('usgs/forks') # sum of forks

main()