# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION ${CMAKE_VERSION}) # this file comes with cmake

# If CMAKE_DISABLE_SOURCE_CHANGES is set to true and the source directory is an
# existing directory in our source tree, calling file(MAKE_DIRECTORY) on it
# would cause a fatal error, even though it would be a no-op.
if(NOT EXISTS "/opt/corrosion")
  file(MAKE_DIRECTORY "/opt/corrosion")
endif()
file(MAKE_DIRECTORY
  "/tmp/nes-1699-tidy/build-rel/_deps/corrosion-build"
  "/tmp/nes-1699-tidy/build-rel/_deps/corrosion-subbuild/corrosion-populate-prefix"
  "/tmp/nes-1699-tidy/build-rel/_deps/corrosion-subbuild/corrosion-populate-prefix/tmp"
  "/tmp/nes-1699-tidy/build-rel/_deps/corrosion-subbuild/corrosion-populate-prefix/src/corrosion-populate-stamp"
  "/tmp/nes-1699-tidy/build-rel/_deps/corrosion-subbuild/corrosion-populate-prefix/src"
  "/tmp/nes-1699-tidy/build-rel/_deps/corrosion-subbuild/corrosion-populate-prefix/src/corrosion-populate-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/tmp/nes-1699-tidy/build-rel/_deps/corrosion-subbuild/corrosion-populate-prefix/src/corrosion-populate-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/tmp/nes-1699-tidy/build-rel/_deps/corrosion-subbuild/corrosion-populate-prefix/src/corrosion-populate-stamp${cfgdir}") # cfgdir has leading slash
endif()
