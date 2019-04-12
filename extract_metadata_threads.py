#!/home/titan/anaconda3/bin/python3

import os
import threading
from queue import Queue
import time
import shutil
import csv
import pyart
from dateutil import parser
import numpy

# lock to serialize console output
lock = threading.Lock()

def extract_metadata(radar_file):
    with lock:
        print("Starting thread : {}" . format(threading.current_thread().name))

    csv_metadata = open("metadata_radar_files.csv", "a")

    # extract file name and extension
    partial_path, file_extension = os.path.splitext(radar_file["radar_file"])
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
            radar = pyart.io.sigmet.read_sigmet(radar_file["radar_file"])
        else:
            radar = pyart.aux_io.read_gamic(radar_file["radar_file"])
    except:
        print("!! Error opening", radar_file["radar_file"])

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

    csv_line = str(radar_file["radar_file"]) + ";"
    csv_line = csv_line + file_name + ";"
    csv_line = csv_line + file_extension + ";"
    csv_line = csv_line + radar_file_full_name + ";"
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
    csv_metadata.close()

    with lock:
        print("Finished thread : {}" . format(threading.current_thread().name))

# The worker thread pulls an item from the queue and processes it
def worker():
    while True:
        radar_file = queue.get()
        extract_metadata(radar_file)
        queue.task_done()

# Create the queue and thread pool
queue = Queue()
for i in range(6):
    t = threading.Thread(target=worker)
    t.daemon = True
    t.start()

# start timer
start = time.time()

# get files from CSV
files_array = []
with open("radar_files.csv") as csv_radar_files:
    reader = csv.DictReader(csv_radar_files, delimiter=';')
    for row in reader:
        queue.put(row, files_array)

# block until all tasks are done
queue.join()

print("Execution time = {0:.5f}" . format(time.time() - start))
