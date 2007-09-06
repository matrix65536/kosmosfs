#
# $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/access/kfs_setup.py#1 $
#
# Use the distutils setup function to build and install the KFS module.
# I am still not clear on how to integrate this with the build system.
# One complication is locating the shared libraries.  A plain make puts
# them in BUILD_DIR, while a 'make libinstall' sticks them in libdir.
# puts them in libdir.  In the first case especially, the location will
# depend on the arguments to make.  So I pass it in as the first argument
# and then remove it from the sys.argv list before calling setup.  It is
# also necessary to set LD_LIBRARY_PATH so that Python can find the
# libraries at run-time.
#
from distutils.core import setup, Extension
import sys

kfs_lib_dir = sys.argv[1]
del sys.argv[1]

kfsext = Extension('kfs',
		include_dirs = ['../libkfsClient', '../..'],
		libraries = ['kfsClient'],
		library_dirs = [kfs_lib_dir],
		sources = ['kfsmodule.cc'])

setup(name = "kfs", version = "1.0",
	description="KFS client module",
	author="Blake Lewis",
	author_email="blake@kosmix.com",
	ext_modules = [kfsext])
