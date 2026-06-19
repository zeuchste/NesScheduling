if(EXISTS "/tmp/nes-1699-tidy/build-rel/nes-memory/tests/unpooled-buffer-test")
  if(NOT EXISTS "/tmp/nes-1699-tidy/build-rel/nes-memory/tests/unpooled-buffer-test[1]_tests.cmake" OR
     NOT "/tmp/nes-1699-tidy/build-rel/nes-memory/tests/unpooled-buffer-test[1]_tests.cmake" IS_NEWER_THAN "/tmp/nes-1699-tidy/build-rel/nes-memory/tests/unpooled-buffer-test" OR
     NOT "/tmp/nes-1699-tidy/build-rel/nes-memory/tests/unpooled-buffer-test[1]_tests.cmake" IS_NEWER_THAN "${CMAKE_CURRENT_LIST_FILE}")
    include("/usr/share/cmake-3.31/Modules/GoogleTestAddTests.cmake")
    gtest_discover_tests_impl(
      TEST_EXECUTABLE [==[/tmp/nes-1699-tidy/build-rel/nes-memory/tests/unpooled-buffer-test]==]
      TEST_EXECUTOR [==[]==]
      TEST_WORKING_DIR [==[/tmp/nes-1699-tidy/build-rel/nes-memory/tests]==]
      TEST_EXTRA_ARGS [==[]==]
      TEST_PROPERTIES [==[ENVIRONMENT;RC_PARAMS=max_success=20]==]
      TEST_PREFIX [==[]==]
      TEST_SUFFIX [==[]==]
      TEST_FILTER [==[]==]
      NO_PRETTY_TYPES [==[FALSE]==]
      NO_PRETTY_VALUES [==[FALSE]==]
      TEST_LIST [==[unpooled-buffer-test_TESTS]==]
      CTEST_FILE [==[/tmp/nes-1699-tidy/build-rel/nes-memory/tests/unpooled-buffer-test[1]_tests.cmake]==]
      TEST_DISCOVERY_TIMEOUT [==[30]==]
      TEST_DISCOVERY_EXTRA_ARGS [==[]==]
      TEST_XML_OUTPUT_DIR [==[]==]
    )
  endif()
  include("/tmp/nes-1699-tidy/build-rel/nes-memory/tests/unpooled-buffer-test[1]_tests.cmake")
else()
  add_test(unpooled-buffer-test_NOT_BUILT unpooled-buffer-test_NOT_BUILT)
endif()
