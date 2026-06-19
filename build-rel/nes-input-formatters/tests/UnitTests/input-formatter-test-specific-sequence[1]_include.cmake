if(EXISTS "/tmp/nes-1699-tidy/build-rel/nes-input-formatters/tests/UnitTests/input-formatter-test-specific-sequence")
  if(NOT EXISTS "/tmp/nes-1699-tidy/build-rel/nes-input-formatters/tests/UnitTests/input-formatter-test-specific-sequence[1]_tests.cmake" OR
     NOT "/tmp/nes-1699-tidy/build-rel/nes-input-formatters/tests/UnitTests/input-formatter-test-specific-sequence[1]_tests.cmake" IS_NEWER_THAN "/tmp/nes-1699-tidy/build-rel/nes-input-formatters/tests/UnitTests/input-formatter-test-specific-sequence" OR
     NOT "/tmp/nes-1699-tidy/build-rel/nes-input-formatters/tests/UnitTests/input-formatter-test-specific-sequence[1]_tests.cmake" IS_NEWER_THAN "${CMAKE_CURRENT_LIST_FILE}")
    include("/usr/share/cmake-3.31/Modules/GoogleTestAddTests.cmake")
    gtest_discover_tests_impl(
      TEST_EXECUTABLE [==[/tmp/nes-1699-tidy/build-rel/nes-input-formatters/tests/UnitTests/input-formatter-test-specific-sequence]==]
      TEST_EXECUTOR [==[]==]
      TEST_WORKING_DIR [==[/tmp/nes-1699-tidy/build-rel/nes-input-formatters/tests/UnitTests]==]
      TEST_EXTRA_ARGS [==[]==]
      TEST_PROPERTIES [==[ENVIRONMENT;RC_PARAMS=max_success=20]==]
      TEST_PREFIX [==[]==]
      TEST_SUFFIX [==[]==]
      TEST_FILTER [==[]==]
      NO_PRETTY_TYPES [==[FALSE]==]
      NO_PRETTY_VALUES [==[FALSE]==]
      TEST_LIST [==[input-formatter-test-specific-sequence_TESTS]==]
      CTEST_FILE [==[/tmp/nes-1699-tidy/build-rel/nes-input-formatters/tests/UnitTests/input-formatter-test-specific-sequence[1]_tests.cmake]==]
      TEST_DISCOVERY_TIMEOUT [==[30]==]
      TEST_DISCOVERY_EXTRA_ARGS [==[]==]
      TEST_XML_OUTPUT_DIR [==[]==]
    )
  endif()
  include("/tmp/nes-1699-tidy/build-rel/nes-input-formatters/tests/UnitTests/input-formatter-test-specific-sequence[1]_tests.cmake")
else()
  add_test(input-formatter-test-specific-sequence_NOT_BUILT input-formatter-test-specific-sequence_NOT_BUILT)
endif()
