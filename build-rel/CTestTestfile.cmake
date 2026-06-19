# CMake generated Testfile for 
# Source directory: /tmp/nes-1699-tidy
# Build directory: /tmp/nes-1699-tidy/build-rel
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test([=[build-runtime-base-image]=] "docker" "build" "--load" "-t" "nes-runtime-base:test" "-f" "/tmp/nes-1699-tidy/docker/runtime/RuntimeBase.dockerfile" "/tmp/nes-1699-tidy/docker/runtime")
set_tests_properties([=[build-runtime-base-image]=] PROPERTIES  FIXTURES_SETUP "RuntimeBaseImage" LABELS "DockerCompose" _BACKTRACE_TRIPLES "/tmp/nes-1699-tidy/CMakeLists.txt;238;add_test;/tmp/nes-1699-tidy/CMakeLists.txt;0;")
subdirs("_deps/corrosion-build")
subdirs("nes-common")
subdirs("nes-configurations")
subdirs("nes-data-types")
subdirs("nes-distributed")
subdirs("nes-executable")
subdirs("nes-frontend")
subdirs("nes-inference")
subdirs("nes-input-formatters")
subdirs("nes-logical-operators")
subdirs("nes-memory")
subdirs("nes-nautilus")
subdirs("nes-network")
subdirs("nes-output-formatters")
subdirs("nes-physical-operators")
subdirs("nes-plugins")
subdirs("nes-query-compiler")
subdirs("nes-query-engine")
subdirs("nes-query-optimizer")
subdirs("nes-runtime")
subdirs("nes-single-node-worker")
subdirs("nes-sinks")
subdirs("nes-sources")
subdirs("nes-sql-parser")
subdirs("nes-systests")
