/*!
 * $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/kfstypes.h#3 $
 *
 * \file kfstypes.h
 * \brief simple typedefs and enums for the KFS metadata server
 * \author Blake Lewis (Kosmix Corp.)
 *
 * Copyright (C) 2006 Kosmix Corp.
 * 
 * This file is part of Kosmos File System (KFS).
 *
 * KFS is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by
 * the Free Software Foundation under version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 *
 */
#if !defined(KFS_TYPES_H)
#define KFS_TYPES_H

#include <cerrno>
#include <cassert>
#include "common/kfstypes.h"

namespace KFS {

/*!
 * \brief KFS metadata types.
 */
enum MetaType {
	KFS_UNINIT,		//!< uninitialized
	KFS_INTERNAL,		//!< internal node
	KFS_FATTR,		//!< file attributes
	KFS_DENTRY,		//!< directory entry
	KFS_CHUNKINFO,		//!< chunk information
	KFS_SENTINEL = 99999	//!< internal use, must be largest
};

/*!
 * \brief KFS file types
 */
enum FileType {
	KFS_NONE,		//!< uninitialized
	KFS_FILE,		//!< plain file
	KFS_DIR			//!< directory
};

/*!
 * \brief KFS lease types
 */
enum LeaseType {
	READ_LEASE,
	WRITE_LEASE
};

}
#endif // !defined(KFS_TYPES_H)
