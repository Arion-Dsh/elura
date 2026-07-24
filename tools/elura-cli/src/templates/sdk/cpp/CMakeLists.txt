cmake_minimum_required(VERSION 3.16)
project(elura_protocol VERSION {{ELURA_VERSION}} LANGUAGES CXX)

add_library(elura_protocol src/elr2.cpp)
add_library(elura::protocol ALIAS elura_protocol)
include(GNUInstallDirs)
target_include_directories(elura_protocol PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_compile_features(elura_protocol PUBLIC cxx_std_20)
set_target_properties(elura_protocol PROPERTIES
  CXX_EXTENSIONS OFF
  CXX_STANDARD_REQUIRED ON
  EXPORT_NAME protocol
)

include(CTest)
if(BUILD_TESTING)
  add_executable(elura_protocol_test tests/elr2_test.cpp)
  target_link_libraries(elura_protocol_test PRIVATE elura_protocol)
  add_test(NAME elura_protocol_test COMMAND elura_protocol_test)
endif()

install(TARGETS elura_protocol
  EXPORT elura_protocol_targets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT elura_protocol_targets
  FILE elura_protocol_targets.cmake
  NAMESPACE elura::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/elura_protocol
)
