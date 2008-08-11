
# Take all the .cc files and build a library out of them
file (GLOB sources . *.cc)

#
# Build both static/dynamic libraries.  Force the linking of all apps
# with a statically linked library.  Since kfsCommon is the symbol
# used everywhere, associate that symbol with the STATIC library.
#

add_library (kfsCommon-shared SHARED ${sources})
add_library (kfsCommon STATIC ${sources})
set_target_properties (kfsCommon-shared PROPERTIES OUTPUT_NAME "kfsCommon")
set_target_properties (kfsCommon PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (kfsCommon-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)

target_link_libraries (kfsCommon ${Log4cpp_LIBRARY} pthread)
target_link_libraries (kfsCommon-shared ${Log4cpp_LIBRARY} pthread)

#
install (TARGETS kfsCommon-shared kfsCommon
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib/static)

install (FILES kfstypes.h kfsdecls.h DESTINATION include/kfs/common)
