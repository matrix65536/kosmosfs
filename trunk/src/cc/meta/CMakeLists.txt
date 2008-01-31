
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

add_library (kfsMeta STATIC ${lib_srcs})
add_library (kfsMeta-shared SHARED ${lib_srcs})
set_target_properties (kfsMeta-shared PROPERTIES OUTPUT_NAME "kfsMeta")
set_target_properties (kfsMeta PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (kfsMeta-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)

add_executable (metaserver metaserver_main.cc)

if (USE_STATIC_LIB_LINKAGE)
   message (STATUS "Linking with static libs")
   add_dependencies (metaserver kfsCommon kfsIO kfsMeta)
   target_link_libraries (metaserver kfsMeta kfsIO kfsCommon pthread)
else (USE_STATIC_LIB_LINKAGE)
   message (STATUS "Linking with dynamic libs")
   add_dependencies (metaserver kfsCommon-shared kfsIO-shared kfsMeta-shared)
   target_link_libraries (metaserver kfsMeta-shared kfsIO-shared kfsCommon-shared pthread)
endif (USE_STATIC_LIB_LINKAGE)

if (NOT APPLE)
  target_link_libraries(kfsMeta rt)
  target_link_libraries(metaserver rt)
endif (NOT APPLE)
#
# Install them
#
install (TARGETS metaserver kfsMeta kfsMeta-shared
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib/static)

