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

# directory where files are stored
dir_raw_files = "/media/nas_uca/radar_raw_data/asuncion/raw"

# recursively get all files in dir_raw_files
pathlist = Path(dir_raw_files).glob("**/*")

# open csv file for write
csv_metadata = open("metadata_" + time.strftime("%Y%m%d%H%M%S") + ".csv", "w")

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

  # open radar file
  try:
    if (radar_file_format == "SIGMET"):
      radar = pyart.io.sigmet.read_sigmet(radar_file)
    else:
      radar = pyart.aux_io.read_gamic(radar_file)
  except:
    print("!! Error opening", radar_file)
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

  csv_line = str(path) + ";"
  csv_line = csv_line + file_name + ";"
  csv_line = csv_line + file_extension + ";"
  csv_line = csv_line + radar_file_full_name + ";"
  csv_line = csv_line + radar_file + ";"
  csv_line = csv_line + radar_file_format + ";"
  csv_line = csv_line + scan_type + ";"
  csv_line = csv_line + str(scan_date) + ";"
  csv_line = csv_line + str(scan_date.year) + ";"
  csv_line = csv_line + str(scan_date.month).zfill(2) + ";"
  csv_line = csv_line + str(scan_date.day).zfill(2) + ";"
  csv_line = csv_line + str(scan_date.hour).zfill(2) + ";"
  csv_line = csv_line + max_reflectivity + ";"
  csv_line = csv_line + min_reflectivity
  csv_metadata.write(csv_line +'\n')

  print("Added to CSV")

csv_metadata.close()
