#!/home/titan/anaconda3/bin/python3

import os
import sys
import time
import shlex
import pyart
import pprint
from pathlib import Path
from dateutil import parser
import numpy

# directory where files to be sorted are store
dir_to_sort = "/media/nas_uca/radar_raw_data/to_sort/"

# target directory (where files are being moved to)
dir_target = "/media/nas_uca/radar_raw_data/asuncion/raw/"

# recursively get all files in dir_to_sort
pathlist = Path(dir_to_sort).glob("**/*")

# open csv file for wirte
csv_radar_files = open("r" + time.strftime("%Y%m%d%H%M%S") + "_radar_files.csv", "w")
csv_radar_files_with_errors = open("r" + time.strftime("%Y%m%d%H%M%S") + "_files_with_errors.csv", "w")

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

  # if extension is not hdf5 or sigmet, get out
  if (not radar_file_full_name.endswith(".mvol") and not file_extension.startswith(".RAW") and not radar_file_full_name.startswith("RAD")):
    print("Does not looks like a radar file, skipping")
    continue;

  radar_file_format = "none"

  # check if file is HDF5-GAMIC
  if (radar_file_full_name.endswith(".mvol")):
    radar_file_format = "HDF5-GAMIC"
    reflectivity_field = "corrected_reflectivity"

  # check if file SIGMET/VAISALA
  # if (file_extension.startswith(".RAW") or radar_file_full_name.startswith("RAD")):
  if (radar_file_full_name.startswith("RAD") and not file_extension.startswith(".nc")):
    radar_file_format = "SIGMET"
    reflectivity_field = "reflectivity"

  print("Radar file format detected is", radar_file_format)

  # open radar file
  try:
    if (radar_file_format == "SIGMET"):
      radar = pyart.io.sigmet.read_sigmet(radar_file)
    else:
      radar = pyart.aux_io.read_gamic(radar_file)
  except:
    print("!! Error opening", radar_file)
    csv_radar_files_with_errors.write(radar_file + "\n")
    continue

  # compute max and min dbz
  min_reflectivity = 0
  try:
    min_reflectivity = numpy.amin(radar.fields[reflectivity_field]["data"])
  except:
    pass

  max_reflectivity = 0
  try:
    max_reflectivity = numpy.amax(radar.fields[reflectivity_field]["data"])
  except:
    pass

  # max/min dbz to string
  max_reflectivity = str(max_reflectivity)
  min_reflectivity = str(min_reflectivity)

  # check if surv or vol scan
  if radar.nsweeps == 1:
    scan_type = 'surveillance'
  else:
    scan_type = 'volumetric'

  print("Format", radar_file_format, "Scan:", scan_type, "Min dBZ", min_reflectivity, "Max dBZ:", max_reflectivity)

  # get scan date
  scan_date_string = radar.time["units"].replace("seconds since ", "")
  scan_date = parser.parse(radar.time["units"].replace("seconds since ", ""))

  # check if scan type folder exists
  path_target = scan_type + '/'
  print("Checking path_target = ", dir_target + path_target)
  if not os.path.exists(dir_target + path_target):
    os.makedirs(dir_target + path_target)

  # check if year folder exists
  path_target = path_target + str(scan_date.year) + "/"
  print("Checking path_target = ", dir_target + path_target)
  if not os.path.exists(dir_target + path_target):
    os.makedirs(dir_target + path_target)

  # check if month folder exists
  path_target = path_target + str(scan_date.month).zfill(2) + "/"
  print("Checking path_target = ", dir_target + path_target)
  if not os.path.exists(dir_target + path_target):
    os.makedirs(dir_target + path_target)

  # check if day folder exists
  path_target = path_target + str(scan_date.day).zfill(2) + "/"
  print("Checking path_target = ", dir_target + path_target)
  if not os.path.exists(dir_target + path_target):
    os.makedirs(dir_target + path_target)

  target_file = dir_target + path_target + radar_file_full_name
  print("Target path is " + target_file)

  # Move file to destination
  move_command = 'mv ' + shlex.quote(radar_file) + ' ' + shlex.quote(target_file)
  os.system(move_command)
  print('Executing: ' + move_command)

  csv_line = str(path) + ";"
  csv_line = csv_line + file_name + ";"
  csv_line = csv_line + file_extension + ";"
  csv_line = csv_line + radar_file_full_name + ";"
  csv_line = csv_line + path_target + radar_file_full_name + ";"
  csv_line = csv_line + radar_file_format + ";"
  csv_line = csv_line + scan_type + ";"
  csv_line = csv_line + str(scan_date) + ";"
  csv_line = csv_line + str(scan_date.year) + ";"
  csv_line = csv_line + str(scan_date.month).zfill(2) + ";"
  csv_line = csv_line + str(scan_date.day).zfill(2) + ";"
  csv_line = csv_line + str(scan_date.hour).zfill(2) + ";"
  csv_line = csv_line + max_reflectivity + ";"
  csv_line = csv_line + min_reflectivity
  csv_radar_files.write(csv_line +'\n')
  # print("Line added to csv:")
  # print(csv_line)

csv_radar_files.close()
