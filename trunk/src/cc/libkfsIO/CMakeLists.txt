
# Take all the .cc files and build a library out of them
file (GLOB sources . *.cc)

add_library (kfsIO STATIC ${sources})
add_library (kfsIO-shared SHARED ${sources})
set_target_properties (kfsIO-shared PROPERTIES OUTPUT_NAME "kfsIO")

set_target_properties (kfsIO PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (kfsIO-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)

add_dependencies (kfsIO kfsCommon)
add_dependencies (kfsIO-shared kfsCommon-shared)
target_link_libraries (kfsIO kfsCommon)
target_link_libraries (kfsIO-shared kfsCommon-shared)
if (NOT APPLE)
   target_link_libraries (kfsIO rt)
   target_link_libraries (kfsIO-shared rt)
endif(NOT APPLE)
#
install (TARGETS kfsIO kfsIO-shared
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib-static)
