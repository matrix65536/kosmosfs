/**
 * $Id: Positionable.java $
 *
 * Created 2007/09/13
 *
 * @author: Sriram Rao (Kosmix Corp.)
 *
 * Copyright (C) 2007 Kosmix Corp.
 *
 * This file is part of Kosmix File System (KFS).
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
 * \brief Interface for positioning a file pointer.
 */

package org.kosmos.access;

import java.io.IOException;

public interface Positionable
{
    /* 
     * Position a file pointer at the specified offset from the
     * beginning of a file.
     */
    int seek(long offset) throws IOException;

    /*
     * Return the current position of the file pointer---the position
     * is from the beginning of the file.
     */
    long tell() throws IOException;
}
