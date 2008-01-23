
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

add_executable (chunkserver ChunkServer_main)

if (USE_STATIC_LIB_LINKAGE)
   add_dependencies (chunkserver kfsCommon kfsIO)
   target_link_libraries (chunkserver chunk kfsIO kfsCommon pthread)
else (USE_STATIC_LIB_LINKAGE)
   add_dependencies (chunkserver kfsCommon-shared kfsIO-shared)
   target_link_libraries (chunkserver chunk-shared kfsIO-shared kfsCommon-shared pthread)
endif (USE_STATIC_LIB_LINKAGE)

if (NOT APPLE)
  target_link_libraries(chunk rt)
  target_link_libraries(chunkserver rt)
endif (NOT APPLE)
#
# Install them
#
install (TARGETS chunkserver chunk
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib/static)

