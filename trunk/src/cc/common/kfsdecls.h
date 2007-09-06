//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/common/kfsdecls.h#3 $
//
// \brief Common declarations of KFS structures
//
// Created 2006/10/20
// Author: Sriram Rao (Kosmix Corp.) 
//
// Copyright (C) 2006 Kosmix Corp.
//
// This file is part of Kosmos File System (KFS).
//
// KFS is free software: you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation under version 3 of the License.
//
// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see
// <http://www.gnu.org/licenses/>.
//
//----------------------------------------------------------------------------

#ifndef COMMON_KFSDECLS_H
#define COMMON_KFSDECLS_H

#include <cassert>
#include <cerrno>
#include <string>
#include <sstream>

namespace KFS {
///
/// Define a server process' location: hostname and the port at which
/// it is listening for incoming connections
///
struct ServerLocation {
    ServerLocation(): hostname(""), port(-1) { }
    ServerLocation(const ServerLocation &other):
	hostname(other.hostname), port(other.port) { }
    ServerLocation(const std::string &h, int p): hostname(h), port(p) { }
    ServerLocation & operator = (const ServerLocation &other) {
	hostname = other.hostname;
	port = other.port;
	return *this;
    }
    void Reset(const char *h, int p) {
	hostname = h;
	port = p;
    }
    bool operator == (const ServerLocation &other) const {
	return hostname == other.hostname && port == other.port;
    }
    bool operator != (const ServerLocation &other) const {
	return hostname != other.hostname || port != other.port;
    }
    bool IsValid() const {
	// Hostname better be non-null and port better
	// be a positive number
	return hostname.compare("") != 0 && port > 0;
    }
    std::string ToString() const {
	std::ostringstream os;

	os << hostname << ' ' << port;
	return os.str();
    }
    void FromString(const std::string &s) {
	std::istringstream is(s);

	is >> hostname;
	is >> port;
    }

    std::string hostname; //!< Location of the server: machine name
    int port; //!< Location of the server: port to connect to
};

}

#endif // COMMON_KFSDECLS_H
