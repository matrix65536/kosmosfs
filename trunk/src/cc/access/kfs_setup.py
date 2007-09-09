#
# $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/access/kfs_setup.py#1 $
#
# Use the distutils setup function to build and install the KFS module.
# Execute this as:
#  python kfs_setup.py ~/code/kfs/build/lib/ build
# and this will build kfs.so in ./build/.../kfs.so
# This needs to be installed /usr/lib64/python/site-packages
# In addition, ~/code/kfs/build/lib needs to be in the LD_LIBRARY_PATH
# After installation, python apps can access kfs.
#
from distutils.core import setup, Extension
import sys

kfs_lib_dir = sys.argv[1]
del sys.argv[1]

kfsext = Extension('kfs',
		include_dirs = ['/home/sriram/code/kosmosfs/src/cc/'],
		libraries = ['kfsClient'],
		library_dirs = [kfs_lib_dir],
		sources = ['KfsModulePy.cc'])

setup(name = "kfs", version = "0.1",
	description="KFS client module",
	author="Blake Lewis",
      	maintainer="Sriram Rao",
	ext_modules = [kfsext])
