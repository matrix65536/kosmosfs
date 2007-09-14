#
# $Id:$
#
# Created 2006/10/20
# Author: Sriram Rao (Kosmix Corp)
#
# Copyright 2006 Kosmix Corp.
#
# This file is part of Kosmos File System (KFS).
#
# Licensed under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#
#

project (KFS)

# Change this to where the install directory is located
set (CMAKE_INSTALL_PREFIX .)

# Change the line to Release to build release binaries
# For servers, build with debugging info; for tools, build Release
#
set (CMAKE_BUILD_TYPE RelWithDebugInfo)

# set (CMAKE_BUILD_TYPE Release)

IF (CMAKE_BUILD_TYPE STREQUAL "Release")
   message(STATUS "Enabling -D NDEBUG flag")
   set(CMAKE_CXX_FLAGS  "-D NDEBUG")
ENDIF(CMAKE_BUILD_TYPE STREQUAL "Release")


# Locate Boost
find_package(Boost REQUIRED)

#
# Find the path to libfuse.so
#

IF (EXISTS "/lib64/libfuse.so")
   SET(Fuse_LIBRARY_DIR "/lib64")
ELSE (EXISTS "/lib64/libfuse.so")
   SET(Fuse_LIBRARY_DIR "")
ENDIF (EXISTS "/lib64/libfuse.so")

# include dirs
include_directories( ${Boost_INCLUDE_DIRS} src/cc)

# get the subdirs we want
add_subdirectory (src/cc/access)
add_subdirectory (src/cc/common)
add_subdirectory (src/cc/meta)
add_subdirectory (src/cc/chunk)
add_subdirectory (src/cc/libkfsClient)
add_subdirectory (src/cc/libkfsIO)
add_subdirectory (src/cc/tools)
add_subdirectory (src/cc/tests)

# set (Fuse_LIBRARY_DIR <provide path>)
IF (NOT ${Fuse_LIBRARY_DIR} STREQUAL "")
   message(STATUS "Found fuse")
   add_subdirectory (src/cc/fuse)
ENDIF (NOT ${Fuse_LIBRARY_DIR} STREQUAL "")
