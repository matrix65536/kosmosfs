
# Take all the .cc files and build a library out of them
file (GLOB sources . *.cc)

add_library (kfsIO STATIC ${sources})
add_library (kfsIO-shared SHARED ${sources})
set_target_properties (kfsIO-shared PROPERTIES OUTPUT_NAME "kfsIO")

set_target_properties (kfsIO PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (kfsIO-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)

add_dependencies (kfsIO common)
target_link_libraries (kfsIO rt)
#
install (TARGETS kfsIO kfsIO-shared
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib-static)
