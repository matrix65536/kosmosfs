
#
# For the library take everything except the *_main.cc files
#

set (lib_srcs
ChunkManager.cc
ChunkServer.cc
ClientManager.cc
ClientSM.cc
KfsOps.cc
LeaseClerk.cc
Logger.cc
MetaServerSM.cc
RemoteSyncSM.cc
Replicator.cc
Utils.cc
)

#
# build both static and dynamic
#
add_library (chunk STATIC ${lib_srcs})
add_library (chunk-shared SHARED ${lib_srcs})
set_target_properties (chunk-shared PROPERTIES OUTPUT_NAME "chunk")
set_target_properties (chunk PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (chunk-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)

add_executable (chunkserver ChunkServer_main.cc)
add_executable (chunkscrubber chunkscrubber_main.cc)

set (exe_files chunkserver chunkscrubber)

foreach (exe_file ${exe_files})
        if (USE_STATIC_LIB_LINKAGE)
           add_dependencies (${exe_file} chunk kfsCommon kfsIO)
           target_link_libraries (${exe_file} chunk kfsIO kfsCommon pthread $(Tcmalloc_LIBRARIES))
        else (USE_STATIC_LIB_LINKAGE)
           add_dependencies (${exe_file} chunk-shared kfsCommon-shared kfsIO-shared)
           target_link_libraries (${exe_file} chunk-shared kfsIO-shared kfsCommon-shared pthread $(Tcmalloc_LIBRARIES))
        endif (USE_STATIC_LIB_LINKAGE)
endforeach (exe_file)

if (APPLE)
  target_link_libraries(chunk-shared kfsIO-shared kfsCommon-shared)
endif (APPLE)

if (NOT APPLE)
  target_link_libraries(chunk rt)
  target_link_libraries(chunkserver rt)
endif (NOT APPLE)

if (CMAKE_SYSTEM_NAME STREQUAL "SunOS")
   target_link_libraries(chunk umem)
   target_link_libraries(chunkserver umem)
endif (CMAKE_SYSTEM_NAME STREQUAL "SunOS")

#
# Install them
#
install (TARGETS ${exe_files} chunk chunk-shared
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib/static)

