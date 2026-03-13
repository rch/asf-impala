#!/bin/bash
# Impala runtime environment for Nix/system hybrid library loading.
#
# Impala binaries link against Nix SASL+JVM (glibc 2.42) AND system libs.
# We use Nix's ld-linux with --library-path so that the library search path
# applies ONLY to the main process, not to child processes (sh, java, etc.)
# which need system glibc.
#
# Usage: source this script, then exec with:
#   exec $IMPALA_LD_LINUX $IMPALA_LD_LIBRARY_PATH <binary> [args...]

IMPALA_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Source the standard Impala config
source "$IMPALA_HOME/bin/impala-config.sh"
. "$IMPALA_HOME/bin/set-ld-library-path.sh"

# Nix store paths
NIX_GLIBC="/nix/store/l0l2ll1lmylczj1ihqn351af2kyp5x19-glibc-2.42-51/lib"
NIX_KRB5="/nix/store/xfjkn6pgqyd4h18fdbf5fshadqyzdklp-krb5-1.22.1-lib/lib"
NIX_SASL="/nix/store/w37v17xs8z2qhcyw6hj8swfyhhr3601v-cyrus-sasl-2.1.28/lib"
NIX_JVM="/nix/store/2c52z0rvldik0rll2xz2ip5dl1c8f3pw-openjdk-21.0.10+7/lib/openjdk/lib/server"

# Toolchain paths
GCC_LIB64="$IMPALA_TOOLCHAIN_PACKAGES_HOME/gcc-10.4.0/lib64"
KUDU_LIB="$IMPALA_TOOLCHAIN_PACKAGES_HOME/kudu-879a8f9e2/debug/lib"

# Build the library path: Nix glibc first (for libc.so.6), system last
# This is passed via --library-path, NOT LD_LIBRARY_PATH, so child
# processes (sh, java) won't inherit it and will use system glibc.
IMPALA_LIB_PATH="$NIX_GLIBC:$GCC_LIB64:$NIX_KRB5:$NIX_SASL:$NIX_JVM:$KUDU_LIB:/lib/x86_64-linux-gnu"

# Export for use by callers
export IMPALA_LD_LINUX="$NIX_GLIBC/ld-linux-x86-64.so.2"
export IMPALA_LD_LIBRARY_PATH="--library-path $IMPALA_LIB_PATH"

# CRITICAL: Clear LD_LIBRARY_PATH and LD_PRELOAD so child processes (sh, java)
# spawned by the Impala binary use system glibc, not Nix glibc.
# The --library-path flag to ld-linux handles the main process's library needs.
# Without this, the Nix devenv shell's LD_LIBRARY_PATH poisons child processes
# with Nix libraries that require glibc 2.38+ while system glibc is 2.35.
unset LD_LIBRARY_PATH
unset LD_PRELOAD
