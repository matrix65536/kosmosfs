#
# $Id:$
#
# Created 2006/10/20
# Author: Sriram Rao (Kosmix Corp)
#
# Copyright (C) 2006 Kosmix Corp.
#
# This file is part of Kosmix File System (KFS).
#
# KFS is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by
# the Free Software Foundation under version 3 of the License.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see
# <http://www.gnu.org/licenses/>.
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

# include dirs
include_directories( ${Boost_INCLUDE_DIRS} src/cc)

# get the subdirs we want
add_subdirectory (src/cc/access)
add_subdirectory (src/cc/common)
# add_subdirectory (src/cc/fuse)
add_subdirectory (src/cc/meta)
add_subdirectory (src/cc/chunk)
add_subdirectory (src/cc/libkfsClient)
add_subdirectory (src/cc/libkfsIO)
add_subdirectory (src/cc/tools)
add_subdirectory (src/cc/tests)

