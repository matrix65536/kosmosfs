
# Build the tools library
add_library (tools MonUtils.cc)
add_dependencies (tools kfsClient)

set (exe_files
cpFs2Kfs
cpKfs2Fs
KfsCp
KfsCat
KfsLs
KfsMkdir
KfsPing
KfsPut
KfsRm
KfsStats
)

#
# Every executable depends on its namesake source with _main.cc
#
foreach (exe_file ${exe_files})
        add_executable (${exe_file} ${exe_file}_main.cc)
        target_link_libraries (${exe_file} tools kfsClient)
endforeach (exe_file)

#
install (TARGETS ${exe_files} tools
        RUNTIME DESTINATION bin/tools
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib-static)
