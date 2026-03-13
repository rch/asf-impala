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

# This file is intended to be sourced to perform common setup for
# the Python 3 $IMPALA_HOME/bin/impala-py* executables.

set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

. $IMPALA_HOME/bin/set-pythonpath.sh

export LD_LIBRARY_PATH="$(python3 "$IMPALA_HOME/infra/python/bootstrap_virtualenv.py" \
  --print-ld-library-path)"
# Nix: toolchain Python's _ssl.so needs libssl.so.3 at runtime. We can't add
# /lib/x86_64-linux-gnu to LD_LIBRARY_PATH because it breaks Nix tools (they'd
# load system glibc 2.35 instead of their Nix glibc 2.42). Instead, symlink
# the system libssl/libcrypto into the toolchain GCC lib64 dir which is already
# in LD_LIBRARY_PATH.
_gcc_lib="$IMPALA_HOME/toolchain/toolchain-packages-gcc10.4.0/gcc-$IMPALA_GCC_VERSION/lib64"
if [[ -d "$_gcc_lib" ]] && [[ ! -e "$_gcc_lib/libssl.so.3" ]] \
    && [[ -e /lib/x86_64-linux-gnu/libssl.so.3 ]]; then
  ln -sf /lib/x86_64-linux-gnu/libssl.so.3 "$_gcc_lib/libssl.so.3"
  ln -sf /lib/x86_64-linux-gnu/libcrypto.so.3 "$_gcc_lib/libcrypto.so.3"
fi

PY_DIR="$(dirname "$0")/../infra/python"
PY_ENV_DIR="${PY_DIR}/env-gcc${IMPALA_GCC_VERSION}-py3"
python3 "$PY_DIR/bootstrap_virtualenv.py" --python3
