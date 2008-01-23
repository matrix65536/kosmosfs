
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

add_library (meta STATIC ${lib_srcs})
add_library (meta-shared SHARED ${lib_srcs})
set_target_properties (meta-shared PROPERTIES OUTPUT_NAME "meta")
set_target_properties (meta PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (meta-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)

add_executable (metaserver metaserver_main)

if (USE_STATIC_LIB_LINKAGE)
   message (STATUS "Linking with static libs")
   add_dependencies (metaserver kfsCommon kfsIO meta)
   target_link_libraries (metaserver meta kfsIO kfsCommon pthread)
else (USE_STATIC_LIB_LINKAGE)
   message (STATUS "Linking with dynamic libs")
   add_dependencies (metaserver kfsCommon-shared kfsIO-shared meta-shared)
   target_link_libraries (metaserver meta-shared kfsIO-shared kfsCommon-shared pthread)
endif (USE_STATIC_LIB_LINKAGE)

if (NOT APPLE)
  target_link_libraries(meta rt)
  target_link_libraries(metaserver rt)
endif (NOT APPLE)
#
# Install them
#
install (TARGETS metaserver meta meta-shared
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib-static)

