cmake_minimum_required(VERSION 3.16)
project(elura_protocol LANGUAGES CXX)

add_library(elura_protocol src/elr2.cpp)
target_include_directories(elura_protocol PUBLIC include)
target_compile_features(elura_protocol PUBLIC cxx_std_17)

include(CTest)
if(BUILD_TESTING)
  add_executable(elura_protocol_test tests/elr2_test.cpp)
  target_link_libraries(elura_protocol_test PRIVATE elura_protocol)
  add_test(NAME elura_protocol_test COMMAND elura_protocol_test)
endif()
