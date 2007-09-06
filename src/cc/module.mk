include magic.mk

# Some aliases for building KFS sub-modules
kfs: kfs/access kfs/libkfsIO kfs/libkfsClient kfs/meta kfs/ChunkServer kfs/tools \
     kfs/tests

kfs_clean: kfs/libkfsIO_clean kfs/libkfsClient_clean kfs/meta_clean \
           kfs/ChunkServer_clean kfs/tools_clean kfs/tests_clean
