//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/libkfsIO/Counter.h#3 $
//
// Created 2006/07/20
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
//
// \brief Counter for statistics gathering.
//
//----------------------------------------------------------------------------

#ifndef LIBKFSIO_COUNTER_H
#define LIBKFSIO_COUNTER_H

#include <string>
#include <sstream>
#include <tr1/unordered_map>

using std::string;
using std::ostringstream;

class Counter;

/// Map from a counter name to the associated Counter object
typedef std::tr1::unordered_map<string, Counter *> CounterMap;
typedef std::tr1::unordered_map<string, Counter *>::const_iterator CounterMapIterator;


/// Counters in KFS are currently setup to track a single "thing".
/// If we need to track multiple related items (such as, network
/// connections and how much I/O is done on them), then we need to
/// have multiple counters one for each and then display the
/// accumulated statistics in some way.
class Counter {
public:
    // XXX: add threshold values for counts

    Counter() : mName(""), mCount(0) { }
    Counter(const char *name) : mName(name), mCount(0) { }
    virtual ~Counter() { }

    /// Print out some information about this counter
    virtual void Show(ostringstream &os) {
        os << mName << ": " << mCount << "\r\n";
    }

    void SetName(const char *name) {
        mName = name;
    }
    
    /// Update the counter 
    virtual void Update(int amount) { mCount += amount; }

    /// Reset the state of this counter
    virtual void Reset() { mCount = 0; }

    const string & GetName() const {
        return mName;
    }
    long long GetValue() const {
        return mCount;
    }
protected:
    /// Name of this counter object
    string mName;
    /// Value of this counter
    long long mCount;
};

class ShowCounter {
    ostringstream &os;
public:
    ShowCounter(ostringstream &o) : os(o) { }
    void operator() (std::tr1::unordered_map<string, Counter *>::value_type v) {
        Counter *c = v.second;
            
        c->Show(os);
    }
};

///
/// Counter manager that tracks all the counters in the system.  The
/// manager can be queried for statistics.
///
class CounterManager {
public:
    CounterManager() { };
    ~CounterManager() {
        
        for (CounterMapIterator iter = mCounters.begin();
             iter != mCounters.end(); ++iter) {
            Counter *c = iter->second;
            delete c;
        }
        mCounters.clear();
    }
    
    /// Add a counter object
    /// @param[in] counter   The counter to be added
    void AddCounter(Counter *counter) {
        mCounters[counter->GetName()] = counter;
    }

    /// Remove a counter object
    /// @param[in] counter   The counter to be removed
    void RemoveCounter(Counter *counter) {
        const string & name = counter->GetName();
        CounterMapIterator iter = mCounters.find(name);

        if (iter == mCounters.end())
            return;

        mCounters.erase(name);
    }

    /// Given a counter's name, retrieve the associated counter
    /// object.
    /// @param[in] name   Name of the counter to be retrieved
    /// @retval The associated counter object if one exists; NULL
    /// otherwise. 
    Counter *GetCounter(const string &name) {
        CounterMapIterator iter = mCounters.find(name);
        Counter *c;

        if (iter == mCounters.end())
            return NULL;
        c = iter->second;
        return c;
    }
    
    /// Print out all the counters in the system, one per line.  Each
    /// line is terminated with a "\r\n".  If there are no counters,
    /// then we print "\r\n".
    void Show(ostringstream &os) {
        if (mCounters.size() == 0) {
            os << "\r\n";
            return;
        }

        for_each(mCounters.begin(), mCounters.end(), ShowCounter(os));
    }

private:
    /// Map that tracks all the counters in the system
    CounterMap  mCounters;
};

#endif // LIBKFSIO_COUNTER_H
