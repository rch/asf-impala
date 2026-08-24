# Licensed to the Apache Software Foundation (ASF).

set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exec/hdf5")
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exec/hdf5")

add_library(ExecHdf5
  hdfs-hdf5-scanner.cc
)
add_dependencies(ExecHdf5 gen-deps)
