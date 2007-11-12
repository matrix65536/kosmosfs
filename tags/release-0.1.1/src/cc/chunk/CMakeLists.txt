
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

add_library (chunk ${lib_srcs})

add_executable (chunkserver ChunkServer_main)
add_dependencies (chunkserver kfsCommon kfsIO)
target_link_libraries (chunkserver chunk kfsIO kfsCommon pthread)
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

