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

# Variables in the file override the default values from impala-config.sh.
# Config changes for release or features branches should go here so they
# can be version controlled but not conflict with changes on the master
# branch.
#
# E.g. to override IMPALA_HADOOP_VERSION, you could uncomment this line:
# IMPALA_HADOOP_VERSION=3.0.0

# signals-360: Use Apache component versions (Hadoop 3.4.1, Hive 3.1.3,
# Iceberg 1.10.1, Thrift 0.11.0, Ranger 2.4.0) instead of CDP builds.
USE_APACHE_COMPONENTS=true
USE_APACHE_HIVE_3=true

# Kudu C++ client: use the toolchain's pre-built version (879a8f9e2) for ABI
# compatibility with the toolchain GCC 10.4.0. The locally-built 1.19.0-SNAPSHOT
# client is compiled with Nix GCC 15/glibc 2.42 and can't link with toolchain binaries.
# Kudu Java client: still uses 1.19.0-SNAPSHOT from ~/.m2/ (see pom.xml overrides).
# IMPALA_KUDU_VERSION=1.19.0-SNAPSHOT  # uncomment when Kudu C++ is rebuilt with toolchain GCC
