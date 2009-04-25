
# Take all the .cc files and build a library out of them
file (GLOB sources . *.cc)

string(TOUPPER KFS_OS_NAME_${CMAKE_SYSTEM_NAME} KFS_OS_NAME)
add_definitions (-D_GNU_SOURCE -D${KFS_OS_NAME} -DKFS_USE_BOOST)

add_library (kfsIO STATIC ${sources})
add_library (kfsIO-shared SHARED ${sources})
set_target_properties (kfsIO-shared PROPERTIES OUTPUT_NAME "kfsIO")

set_target_properties (kfsIO PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (kfsIO-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)

add_dependencies (kfsIO kfsCommon)
add_dependencies (kfsIO-shared kfsCommon-shared)
target_link_libraries (kfsIO kfsCommon pthread z)
target_link_libraries (kfsIO-shared kfsCommon-shared pthread z)
if (NOT APPLE)
   target_link_libraries (kfsIO rt)
   target_link_libraries (kfsIO-shared rt)
endif(NOT APPLE)
#
install (TARGETS kfsIO kfsIO-shared
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib/static)
