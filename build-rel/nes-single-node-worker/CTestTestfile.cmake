# CMake generated Testfile for 
# Source directory: /tmp/nes-1699-tidy/nes-single-node-worker
# Build directory: /tmp/nes-1699-tidy/build-rel/nes-single-node-worker
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test([=[worker-offline-test]=] "env" "NES_WORKER=/tmp/nes-1699-tidy/build-rel/nes-single-node-worker/nes-single-node-worker" "env" "NES_WORKER_TESTDATA=/tmp/nes-1699-tidy/nes-single-node-worker/tests" "/usr/bin/bats" "-x" "--verbose-run" "/tmp/nes-1699-tidy/nes-single-node-worker/tests/offline.bats")
set_tests_properties([=[worker-offline-test]=] PROPERTIES  _BACKTRACE_TRIPLES "/tmp/nes-1699-tidy/nes-single-node-worker/CMakeLists.txt;38;add_test;/tmp/nes-1699-tidy/nes-single-node-worker/CMakeLists.txt;0;")
subdirs("src")
