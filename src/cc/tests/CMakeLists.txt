

set (exe_files
KfsDataGen
KfsDirFileTester
KfsPerfReader
KfsPerfWriter
KfsReader
KfsSeekWrite
KfsTrunc
KfsWriter
mkfstree
KfsRW
)

#
# Every executable depends on its namesake source with _main.cc
#
foreach (exe_file ${exe_files})
        add_executable (${exe_file} ${exe_file}_main.cc)
        if (USE_STATIC_LIB_LINKAGE)
                add_dependencies (${exe_file} kfsClient)
                target_link_libraries (${exe_file} tools kfsClient $(Tcmalloc_LIBRARIES))
        else (USE_STATIC_LIB_LINKAGE)
                add_dependencies (${exe_file} kfsClient-shared)
                target_link_libraries (${exe_file} tools-shared kfsClient-shared $(Tcmalloc_LIBRARIES))
        endif (USE_STATIC_LIB_LINKAGE)

endforeach (exe_file)

#
install (TARGETS ${exe_files}
        RUNTIME DESTINATION bin/tests)

# Enable some automated tests
ADD_TEST(unittest-write KfsPerfWriter "-p" ${PROJECT_BINARY_DIR}/conf/KfsClient-local.prp "-m" 128 "-f" "/foo.1")
ADD_TEST(unittest-read KfsPerfReader "-p" ${PROJECT_BINARY_DIR}/conf/KfsClient-local.prp "-m" 128 "-f" "/foo.1")

# performance testing
ADD_TEST(perftest-write KfsPerfWriter "-p" ${PROJECT_BINARY_DIR}/conf/KfsClient-local.prp "-m" 512 "-f" "/foo.1")
ADD_TEST(perftest-read KfsPerfReader "-p" ${PROJECT_BINARY_DIR}/conf/KfsClient-local.prp "-m" 512 "-f" "/foo.1")

