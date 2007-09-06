/*!
 * $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/startup.h#3 $
 *
 * Copyright (C) 2006 Kosmix Corp.
 * Author: Blake Lewis (Kosmix Corp.)
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
 * \file startup.h
 * \brief code for starting up the metadata server
 */
#if !defined(KFS_STARTUP_H)
#define KFS_STARTUP_H

#include <string>

namespace KFS {

extern void kfs_startup(const std::string &logdir, const std::string &cpdir);

}
#endif // !defined(KFS_STARTUP_H)
