

set (exe_files
KfsDataGen
KfsDirFileTester
KfsPerfReader
KfsPerfWriter
KfsReader
KfsSeekWrite
KfsTrunc
KfsWriter
)

#
# Every executable depends on its namesake source with _main.cc
#
foreach (exe_file ${exe_files})
        add_executable (${exe_file} ${exe_file}_main.cc)
        add_dependencies (${exe_file} kfsClient)
        target_link_libraries (${exe_file} tools kfsClient)
endforeach (exe_file)

#
install (TARGETS ${exe_files}
        RUNTIME DESTINATION bin/tests)

