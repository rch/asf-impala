# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Find the native Kerberos includes and libraries
#
#  KERBEROS_INCLUDE_DIR       - Where to find krb5.h, etc.
#  KERBEROS_LIBRARY           - Full path to libkrb5 (never bare -lkrb5 alone on Nix)
#  KERBEROS_GSSAPI_LIBRARY    - Full path to libgssapi_krb5 (required; Kudu/Impala link it)
#  KERBEROS_FOUND             - True if krb5 + gssapi_krb5 found.
#
# Hints: KRB5_ROOT / ENV{KRB5_ROOT}, CMAKE_PREFIX_PATH, CMAKE_LIBRARY_PATH
# (devenv/Nix sets these from pkgs.krb5.dev + pkgs.krb5.lib).

set(_KRB5_SEARCH_DIRS)
if (KRB5_ROOT)
  list(APPEND _KRB5_SEARCH_DIRS ${KRB5_ROOT})
endif()
if (DEFINED ENV{KRB5_ROOT} AND NOT "$ENV{KRB5_ROOT}" STREQUAL "")
  list(APPEND _KRB5_SEARCH_DIRS $ENV{KRB5_ROOT})
endif()

if (_KRB5_SEARCH_DIRS)
  find_path(KERBEROS_INCLUDE_DIR krb5.h
    PATHS ${_KRB5_SEARCH_DIRS}
    PATH_SUFFIXES include
    NO_DEFAULT_PATH)
  find_library(KERBEROS_LIBRARY NAMES krb5
    PATHS ${_KRB5_SEARCH_DIRS}
    PATH_SUFFIXES lib lib64
    NO_DEFAULT_PATH)
endif()

# Fall back to CMAKE_PREFIX_PATH / system (still prefer full paths from find_library)
if (NOT KERBEROS_INCLUDE_DIR)
  find_path(KERBEROS_INCLUDE_DIR krb5.h PATH_SUFFIXES include)
endif()
if (NOT KERBEROS_LIBRARY)
  find_library(KERBEROS_LIBRARY NAMES krb5 PATH_SUFFIXES lib lib64)
endif()

# gssapi_krb5 must resolve to a full path. Bare -lgssapi_krb5 fails under Nix/devenv
# because gold does not use DT_RUNPATH as a link-time -L search path.
if (KERBEROS_LIBRARY)
  get_filename_component(_KERBEROS_LIBDIR "${KERBEROS_LIBRARY}" DIRECTORY)
  find_library(KERBEROS_GSSAPI_LIBRARY NAMES gssapi_krb5
    PATHS ${_KERBEROS_LIBDIR} ${_KRB5_SEARCH_DIRS}
    PATH_SUFFIXES lib lib64
    NO_DEFAULT_PATH)
endif()
if (NOT KERBEROS_GSSAPI_LIBRARY)
  find_library(KERBEROS_GSSAPI_LIBRARY NAMES gssapi_krb5 PATH_SUFFIXES lib lib64)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Kerberos DEFAULT_MSG
  KERBEROS_LIBRARY KERBEROS_GSSAPI_LIBRARY KERBEROS_INCLUDE_DIR)

mark_as_advanced(KERBEROS_LIBRARY KERBEROS_GSSAPI_LIBRARY KERBEROS_INCLUDE_DIR)
