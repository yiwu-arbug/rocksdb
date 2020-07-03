cmake_minimum_required(VERSION 3.0)
project(cloud)
enable_language(CXX)
enable_language(C)

# Manage absence of rocksdb code dependence

if (NOT ROCKSDB_GIT_REPO)
  set(ROCKSDB_GIT_REPO "https://github.com/pingcap/rocksdb.git")
endif()

if (NOT ROCKSDB_GIT_BRANCH)
  set(ROCKSDB_GIT_BRANCH "6.4.tikv")
endif()

if (NOT DEFINED ROCKSDB_DIR)
  if (GIT_FOUND)
    if (WIN32)
      execute_process(COMMAND $ENV{COMSPEC} /C ${GIT_EXECUTABLE} clone --branch=${ROCKSDB_GIT_BRANCH} ${ROCKSDB_GIT_REPO})
    else()
      execute_process(COMMAND ${GIT_EXECUTABLE} clone --branch=${ROCKSDB_GIT_BRANCH} ${ROCKSDB_GIT_REPO})
    endif()
    set(ROCKSDB_DIR "${CMAKE_BINARY_DIR}/rocksdb")
  endif()
endif()

if (NOT DEFINED ROCKSDB_DIR)
  message(FATAL_ERROR "ROCKSDB_DIR is not defined.")
endif()

# Append rocksdb modules
list(APPEND CMAKE_MODULE_PATH "${ROCKSDB_DIR}/cmake/modules/")
include(cmake/rocksdb_flags.cmake)

include_directories(${ROCKSDB_DIR})
include_directories(${ROCKSDB_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${PROJECT_SOURCE_DIR}/util)

file(GLOB SOURCES src/*.cc)
file(GLOB TEST_SOURCES src/*test.cc)
list(REMOVE_ITEM SOURCES ${TEST_SOURCES})

add_library(cloud STATIC ${SOURCES} $<TARGET_OBJECTS:cloud_env>)

option(WITH_CLOUD_TESTS "Build with tests." ON)
option(WITH_CLOUD_TOOLS "Build with tools." ON)

if(GIT_FOUND AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/.git")
  if(WIN32)
    execute_process(COMMAND $ENV{COMSPEC} /C ${GIT_EXECUTABLE} rev-parse HEAD OUTPUT_VARIABLE GIT_SHA)
  else()
    execute_process(COMMAND ${GIT_EXECUTABLE} rev-parse HEAD OUTPUT_VARIABLE GIT_SHA)
  endif()
else()
  set(GIT_SHA 0)
endif()
string(REGEX REPLACE "[^0-9a-f]+" "" GIT_SHA "${GIT_SHA}")

set(CLOUD_ENV_CC ${CMAKE_BINARY_DIR}/cloud_env.cc)
add_library(cloud_build_version OBJECT ${CLOUD_ENV})
target_include_directories(cloud_env PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/cloud)

include(GNUInstallDirs)
install(DIRECTORY include/rocksdb/cloud
  COMPONENT devel
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)
install(TARGETS cloud
  COMPONENT devel
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)