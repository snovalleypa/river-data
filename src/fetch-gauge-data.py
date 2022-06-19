#!/usr/bin/env python3
import hydrofunctions as hf
import pandas as pd
import numpy as np
import argparse
import requests
from datetime import datetime
from pytz import timezone
import json
import sys
import os

## to get stage altitude and available date ranges
##https://waterservices.usgs.gov/nwis/site/?format=rdb&sites=12141300&seriesCatalogOutput=true&siteStatus=all

# Floodzilla API: https://prodplanreadingsvc.azurewebsites.net/api/GetGageReadingsUTC?regionId=1&id=SVPA-25&fromDateTime=2022-06-10T00:30:59Z&toDateTime=2022-06-12T00:30:59Z&getMinimalReadings=true&includeStatus=true&includePredictions=true


def data_path(source:str, usgs_id, data_year):
  return 'data/{}/{}/{}-{}.parquet'.format(source.lower(),usgs_id, usgs_id, data_year)

def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

def get_USGS_location_year(usgs_id, data_year):
  start = '{}-10-01'.format(data_year-1)
  end = '{}-09-30'.format(data_year)
  data = hf.NWIS(usgs_id, 'iv', start, end)
  df = data.df()
  df = df.rename(columns={
      "USGS:{}:00060:00000".format(usgs_id): "discharge",
      "USGS:{}:00060:00000_qualifiers".format(usgs_id): "discharge_qualifiers", 
      "USGS:{}:00065:00000".format(usgs_id): "stage",
      "USGS:{}:00065:00000_qualifiers".format(usgs_id): "stage_qualifiers"
  })
  clean_write_data(df, "USGS", usgs_id, data_year)

def get_SVPA_location_year(svpa_id, data_year):
  start = '{}-10-01T00:00:00Z'.format(data_year-1)
  end = '{}-09-30T00:00:00Z'.format(data_year)
  params = {
    "regionId":"1",
    "id": svpa_id,
    "fromDateTime":start,
    "toDateTime":end,
    "returnUTC":"true",
    "getMinimalReadings":"true",
  }
  print("fetching: {} for {}".format(svpa_id, data_year))
  response = requests.get("https://prodplanreadingsvc.azurewebsites.net/api/GetGageReadingsUTC", params)
  readings = response.json()['readings']
  if len(readings) == 0:
    print("no data")
    return 
  df = pd.DataFrame(readings)
  df = df.rename(columns={
      "waterHeight": "stage",
  })
  df['stage_qualifiers'] = ''
  df.drop(columns=['isDeleted', 'isMissing'], inplace=True, errors='ignore')
  df.index = pd.to_datetime(df.pop("timestamp"), utc=True)
  df.index.name = 'datetimeUTC'
  clean_write_data(df, "SVPA", svpa_id, data_year)

def clean_write_data(df, source, id, data_year):
  df['location'] = id
  if 'stage' not in df.columns:
    df['stage'] = np.nan
    df['stage_qualifiers'] = ''
  if 'discharge' not in df.columns:
    df['discharge'] = np.nan
    df['discharge_qualifiers'] = ''
  path = data_path(source, id, data_year)
  ensure_dir(path)
  print('writing: {}'.format(path))
  df.to_parquet(path)


def load(locations, years):
  for data_year in years:
    for loc in locations:
      loc_id = loc['id']
      source = loc['source']
      if source=="USGS":
        try:
          get_USGS_location_year(loc_id, data_year)
        except:
          try:
            print('retry: {}-{}'.format(loc_id, data_year))
            get_USGS_location_year(usgs_id, data_year)
          except:
            print('FAILED: {}-{}'.format(loc_id, data_year))
      else:
        get_SVPA_location_year(loc_id, data_year)

def fix(locations, years):
  for data_year in years:
    for loc in locations:
      usgs_id = loc['id']
      path = data_path(usgs_id, data_year)
      try:
        df = pd.read_parquet(path)
      except:
        print('no-file: {}'.format(path))
        continue
      if 'stage' not in df.columns:
        df['stage'] = np.nan
        df['stage_qualifiers'] = ''
      if 'discharge' not in df.columns:
        df['discharge'] = np.nan
        df['discharge_qualifiers'] = ''
      print(path)
      df.to_parquet(path)


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("-y", "--floodyears", type=str,
                        help="ex. 2001-2022", required=True)
  parser.add_argument("-id", "--locid", type=str,
                        help="ex. SVPA-26. Defaults to all.", required=False)
  parser.add_argument("-s", "--source", type=str,
                        help="SVPA or USGS", required=False)

  cmd_options = parser.parse_args(sys.argv[1:])
  years = cmd_options.floodyears.split("-")
  years_int = [int(y) for y in years]
  years_range = range(years_int[0], years_int[0] +1) if len(years_int)==1 else range(years_int[0], years_int[1]+1)
  years_list = list(years_range)
  with open('locations.json') as json_file:
    data = json.load(json_file)
    locations = data['locations']
    filtered_locations =  locations if (cmd_options.locid == None) else list(filter(lambda loc: loc['id'] == cmd_options.locid , locations))
    filtered_locations =  filtered_locations if (cmd_options.source == None) else list(filter(lambda loc: loc['source'] == cmd_options.source , filtered_locations))
    load(filtered_locations, years_range)
  

main()