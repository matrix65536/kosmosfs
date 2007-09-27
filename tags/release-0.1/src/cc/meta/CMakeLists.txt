
#
# For the library take everything except the *_main.cc files
#
set (lib_srcs
checkpoint.cc
ChunkReplicator.cc
ChunkServer.cc
ChunkServerFactory.cc
ClientSM.cc
entry.cc
kfsops.cc
kfstree.cc
LayoutManager.cc
LeaseCleaner.cc
logger.cc
meta.cc
NetDispatch.cc
replay.cc
request.cc
restore.cc
startup.cc
util.cc
)

add_library (meta ${lib_srcs})

add_executable (metaserver metaserver_main)
add_dependencies (metaserver kfsCommon kfsIO meta)
target_link_libraries (metaserver meta kfsIO kfsCommon pthread rt)
#
# Install them
#
install (TARGETS metaserver meta
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib/static)

