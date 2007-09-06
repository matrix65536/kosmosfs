
#
# Build the fuse binary; build it only on demand
#
add_executable (kfs_fuse EXCLUDE_FROM_ALL kfs_fuse_main.cc)
target_link_libraries (kfs_fuse kfsClient kfsIO common pthread rt)

#
install (TARGETS kfs_fuse
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib-static)
