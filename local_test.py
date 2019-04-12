import pyart
import numpy

# radar_file = "RAD081019030233.RAW47X1"
radar_file = "2017-08-08--11350800.mvol"

# radar = pyart.io.read(radar_file)
radar = pyart.aux_io.read_gamic(radar_file)
print(radar.info())

# max_reflectivity = numpy.amax(radar.fields["reflectivity"]["data"])
# min_reflectivity = numpy.amin(radar.fields["reflectivity"]["data"])

# print('max_reflectivity is ' + str(max_reflectivity))
# print('min_reflectivity is ' + str(min_reflectivity))
