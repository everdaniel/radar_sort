#!/home/titan/anaconda3/bin/python3

import os
import sys
import time
from pathlib import Path

# directory where files are stored
dir_raw_files = "/media/nas_uca/radar_raw_data/asuncion/raw"

# recursively get all files in dir_raw_files
pathlist = Path(dir_raw_files).glob("**/*")

# open csv file for write
csv_radar_files = open("radar_files_" + time.strftime("%Y%m%d%H%M%S") + ".csv", "w")

# iterate over each file
for path in pathlist:
  # get file path in string
  radar_file = str(path)

  # let user know what we are doing
  print("==> Processing " + radar_file)

  if (os.path.isdir(radar_file)):
    print("* Skipping, path is folder")
    continue

  # extract file name and extension
  partial_path, file_extension = os.path.splitext(radar_file)
  path_parts = partial_path.split("/")
  file_name = path_parts[-1]
  radar_file_full_name = file_name + file_extension
  print("Filename:", file_name, "File extension:", file_extension, "Full radar file name:", radar_file_full_name)

  # check if file is HDF5-GAMIC
  if (radar_file_full_name.endswith(".mvol")):
    radar_file_format = "HDF5-GAMIC"
    reflectivity_field = "corrected_reflectivity"

  # check if file SIGMET/VAISALA
  if (radar_file_full_name.startswith("RAD")):
    radar_file_format = "SIGMET"
    reflectivity_field = "reflectivity"

  print("Radar file format detected is", radar_file_format)

  csv_line = str(path) + ";"
  csv_line = csv_line + file_name + ";"
  csv_line = csv_line + file_extension + ";"
  csv_line = csv_line + radar_file_full_name + ";"
  csv_line = csv_line + radar_file + ";"
  csv_line = csv_line + radar_file_format + ";"
  csv_radar_files.write(csv_line +'\n')

  print("Added to CSV")

csv_radar_files.close()
