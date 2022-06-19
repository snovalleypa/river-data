import hydrofunctions as hf
import pandas as pd
import numpy as np
import json
import os

def data_file_path():
  return 'data/usgs/forks/forks.parquet'

def data_path(usgs_id):
  return 'data/usgs/{}'.format(usgs_id)

def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)


def main():
  forks = ['12143400','12141300','12142000']
  df = pd.read_parquet(data_path(forks[0]))  
  df = df.join(pd.read_parquet(data_path(forks[1])), rsuffix=forks[1])
  df = df.join(pd.read_parquet(data_path(forks[2])), rsuffix=forks[2])
  print(df.columns)
  df = df[
      df['location{}'.format(forks[1])].notnull() & df['location{}'.format(forks[2])].notnull()
    ]
  df['location'] = "FORKS"
  df['discharge'] = df['discharge'] + df['discharge{}'.format(forks[1])] + df['discharge{}'.format(forks[2])]
  df = df[["location", "discharge", "discharge_qualifiers"]]
  ensure_dir(data_file_path())
  df.to_parquet(data_file_path())

main()