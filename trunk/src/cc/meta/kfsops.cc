/*!
 * $Id$ 
 *
 * \file kfsops.cc
 * \brief KFS file system operations.
 * \author Blake Lewis and Sriram Rao (Kosmix Corp.)
 *
 * Copyright 2006 Kosmix Corp.
 *
 * This file is part of Kosmos File System (KFS).
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
#include "kfstypes.h"
#include "kfstree.h"
#include "util.h"
#include "LayoutManager.h"

#include <algorithm>
#include <functional>
#include <boost/lexical_cast.hpp>
#include "common/log.h"
#include "common/config.h"

using std::mem_fun;
using std::for_each;
using std::find_if;
using std::lower_bound;

using std::cout;
using std::endl;

using namespace KFS;

const string DUMPSTERDIR("dumpster");

/*!
 * \brief Make a dumpster directory into which we can rename busy
 * files.  When the file is non-busy, it is nuked from the dumpster.
 */
void
KFS::makeDumpsterDir()
{
	fid_t dummy = 0;
	metatree.mkdir(ROOTFID, DUMPSTERDIR, &dummy);
}

/*!
 * \brief Cleanup the dumpster directory on startup.  Also, if
 * the dumpster doesn't exist, make one.
 */
void
KFS::emptyDumpsterDir()
{
	makeDumpsterDir();
	metatree.cleanupDumpster();
}

/*!
 * \brief check file name for legality
 *
 * Legal means nonempty and not containing any slashes.
 *
 * \param[in]	name to check
 * \return	true if name is legal
 */
static bool
legalname(const string name)
{
	return (!name.empty() && name.find('/', 0) == string::npos);
}

/*!
 * \brief see whether path is absolute
 */
static bool
absolute(const string path)
{
	return (path[0] == '/');
}

/*!
 * \brief common code for create and mkdir
 * \param[in] dir	fid of parent directory
 * \param[in] fname	name of object to be created
 * \param[in] type	file or directory
 * \param[in] myID	fid of new object
 * \param[in] numReplicas desired degree of replication for file
 *
 * Create a directory entry and file attributes for the new object.
 * But don't create attributes for "." and ".." links, since these
 * share the directory's attributes.
 */
int
Tree::link(fid_t dir, const string fname, FileType type, fid_t myID,
		int16_t numReplicas)
{
	assert(legalname(fname));
	MetaDentry *dentry = new MetaDentry(dir, fname, myID);
	insert(dentry);
	if (fname != "." && fname != "..") {
		MetaFattr *fattr = new MetaFattr(type, dentry->id(), numReplicas);
		insert(fattr);
	}
	return 0;
}

/*!
 * \brief create a new file
 * \param[in] dir	file id of the parent directory
 * \param[in] fname	file name
 * \param[out] newFid	id of new file
 * \param[in] numReplicas desired degree of replication for file
 * \param[in] exclusive  model the O_EXCL flag of open()
 *
 * \return		status code (zero on success)
 */
int
Tree::create(fid_t dir, const string &fname, fid_t *newFid, 
		int16_t numReplicas, bool exclusive)
{
	if (!legalname(fname)) {
		KFS_LOG_DEBUG("Bad file name %s", fname.c_str());
		return -EINVAL;
	}

	MetaFattr *fa = lookup(dir, fname);
	if (fa != NULL) {
		if (fa->type != KFS_FILE)
			return -EISDIR;

		// Model O_EXECL behavior in create: if the file exists
		// and exclusive is specified, fail the create.
		if (exclusive)
			return -EEXIST;

		int status = remove(dir, fname);
		if (status == -EBUSY) {
			KFS_LOG_DEBUG("Remove failed as file is busy");
			return status;
		}
		assert(status == 0);
	}

	if (*newFid == 0)
		*newFid = fileID.genid();
	return link(dir, fname, KFS_FILE, *newFid, numReplicas);
}

/*!
 * \brief common code for remove and rmdir
 * \param[in] dir	fid of parent directory
 * \param[in] fname	name of item to be removed
 * \param[in] fa	attributes for removed item
 * \pamam[in] save_fa	don't delete attributes if true
 *
 * save_fa prevents multiple deletions when removing
 * the "." and ".." links to a directory.
 */
void
Tree::unlink(fid_t dir, const string fname, MetaFattr *fa, bool save_fa)
{
	MetaDentry dentry(dir, fname, fa->id());
	int UNUSED_ATTR status = del(&dentry);
	assert(status == 0);
	if (!save_fa) {
		status = del(fa);
		assert(status == 0);
	}
}

/*!
 * \brief remove a file
 * \param[in] dir	file id of the parent directory
 * \param[in] fname	file name
 * \return		status code (zero on success)
 */
int
Tree::remove(fid_t dir, const string &fname)
{
	MetaFattr *fa = lookup(dir, fname);
	if (fa == NULL)
		return -ENOENT;
	if (fa->type != KFS_FILE)
		return -EISDIR;
	if (fa->chunkcount > 0) {
		vector <MetaChunkInfo *> chunkInfo;
		getalloc(fa->id(), chunkInfo);
		assert(fa->chunkcount == (long long)chunkInfo.size());
		if (gLayoutManager.IsValidLeaseIssued(chunkInfo)) {
			// put the file into dumpster
			int status = moveToDumpster(dir, fname);
			KFS_LOG_DEBUG("Moving %s to dumpster", fname.c_str());
			return status;
		}
		// fire-away...
		for_each(chunkInfo.begin(), chunkInfo.end(),
			 mem_fun(&MetaChunkInfo::DeleteChunk));
	}
	unlink(dir, fname, fa, false);
	return 0;
}

/*!
 * \brief create a new directory
 * \param[in] dir	file id of the parent directory
 * \param[in] dname	name of new directory
 * \param[out] newFid	id of new directory
 * \return		status code (zero on success)
 */
int
Tree::mkdir(fid_t dir, const string &dname, fid_t *newFid)
{

	if (!legalname(dname) && dir != ROOTFID && dname != "/")
		return -EINVAL;

	if (lookup(dir, dname) != NULL)
		return -EEXIST;

	fid_t myID = *newFid;
	if (myID == 0)
		myID = (dname == "/") ? dir : fileID.genid();
	MetaDentry *dentry = new MetaDentry(dir, dname, myID);
	MetaFattr *fattr = new MetaFattr(KFS_DIR, dentry->id(), 1);
	insert(dentry);
	insert(fattr);
	int status = link(myID, ".", KFS_DIR, myID, 1);
	if (status != 0)
		panic("link(.)",false);
	status = link(myID, "..", KFS_DIR, dir, 1);
	if (status != 0)
		panic("link(..)", false);

	*newFid = myID;
	return 0;
}

/*!
 * \brief check whether a directory is empty
 * \param[in] dir	file ID of the directory
 */
bool
Tree::emptydir(fid_t dir)
{
	vector <MetaDentry *> v;
	readdir(dir, v);

	return (v.size() == 2);
}

/*!
 * \brief remove a directory
 * \param[in] dir	file id of the parent directory
 * \param[in] dname	name of directory
 * \return		status code (zero on success)
 */
int
Tree::rmdir(fid_t dir, const string &dname)
{
	MetaFattr *fa = lookup(dir, dname);

	if ((dir == ROOTFID) && (dname == DUMPSTERDIR)) {
		KFS_LOG_INFO(" Preventing removing dumpster (%s)",
					dname.c_str());
		return -EPERM;
	}

	if (fa == NULL)
		return -ENOENT;
	if (fa->type != KFS_DIR)
		return -ENOTDIR;
	if (dname == "." || dname == "..")
		return -EPERM;

	fid_t myID = fa->id();
	if (!emptydir(myID))
		return -ENOTEMPTY;

	unlink(myID, ".", fa, true);
	unlink(myID, "..", fa, true);
	unlink(dir, dname, fa, false);
	return 0;
}

/*!
 * \brief return attributes for the specified object
 * \param[in] fid	the object's file id
 * \return		pointer to the attributes
 */
MetaFattr *
Tree::getFattr(fid_t fid)
{
	const Key fkey(KFS_FATTR, fid);
	Node *l = findLeaf(fkey);
	return (l == NULL) ? NULL : l->extractMeta<MetaFattr>(fkey);
}

MetaDentry *
Tree::getDentry(fid_t dir, const string &fname)
{
	vector <MetaDentry *> v;

	if (readdir(dir, v) != 0)
		return NULL;

	vector <MetaDentry *>::iterator d;
	d = find_if(v.begin(), v.end(), DirMatch(fname));
	return (d == v.end()) ? NULL : *d;
}

/*!
 * \brief look up a file name and return its attributes
 * \param[in] dir	file id of the parent directory
 * \param[in] fname	file name that we are looking up
 * \return		file attributes or NULL if not found
 */
MetaFattr *
Tree::lookup(fid_t dir, const string &fname)
{
	MetaDentry *d = getDentry(dir, fname);
	if (d == NULL)
		return NULL;
	MetaFattr *fa = getFattr(d->id());
	assert(fa != NULL);
	return fa;
}

/*!
 * \brief repeatedly apply Tree::lookup to an entire path
 * \param[in] rootdir	file id of starting directory
 * \param[in] path	the path to look up
 * \return		attributes of the last component (or NULL)
 */
MetaFattr *
Tree::lookupPath(fid_t rootdir, const string &path)
{
	string component;
	bool isabs = absolute(path);
	fid_t dir = (rootdir == 0 || isabs) ? ROOTFID : rootdir;
	string::size_type cstart = isabs ? 1 : 0;
	string::size_type slash = path.find('/', cstart);

	if (path.size() == cstart)
		return lookup(dir, "/");

	while (slash != string::npos) {
		component.assign(path, cstart, slash - cstart);
		MetaFattr *fa = lookup(dir, component);
		if (fa == NULL)
			return NULL;
		dir = fa->id();
		cstart = slash + 1;
		slash = path.find('/', cstart);
	}

	component.assign(path, cstart, path.size() - cstart);
	return lookup(dir, component);
}

/*!
 * \brief read the contents of a directory
 * \param[in] dir	file id of directory
 * \param[out] v	vector of directory entries
 * \return		status code
 */
int
Tree::readdir(fid_t dir, vector <MetaDentry *> &v)
{
	const Key dkey(KFS_DENTRY, dir);
	Node *l = findLeaf(dkey);
	if (l == NULL)
		return -ENOENT;
	extractAll(l, dkey, v);
	assert(v.size() >= 2);
	return 0;
}

/*!
 * \brief return a file's chunk information (if any)
 * \param[in] file	file id for the file
 * \param[out] v	vector of MetaChunkInfo results
 * \return		status code
 */
int
Tree::getalloc(fid_t file, vector <MetaChunkInfo *> &v)
{
	const Key ckey(KFS_CHUNKINFO, file, Key::MATCH_ANY);
	Node *l = findLeaf(ckey);
	if (l != NULL)
		extractAll(l, ckey, v);
	return 0;
}

/*!
 * \brief return the specific chunk information from a file
 * \param[in] file	file id for the file
 * \param[in] offset	offset in the file
 * \param[out] c	MetaChunkInfo
 * \return		status code
 */
int
Tree::getalloc(fid_t file, chunkOff_t offset, MetaChunkInfo **c)
{
	// Allocation information is stored for offset's in the file that
	// correspond to chunk boundaries.
	chunkOff_t boundary = chunkStartOffset(offset);
	const Key ckey(KFS_CHUNKINFO, file, boundary);
	Node *l = findLeaf(ckey);
	if (l == NULL)
		return -ENOENT;
	*c = l->extractMeta<MetaChunkInfo>(ckey);
	return 0;
}

class ChunkIdMatch {
	chunkId_t myid;
public:
	ChunkIdMatch(seq_t c) : myid(c) { }
	bool operator() (MetaChunkInfo *m) {
		return m->chunkId == myid;	
	}
};

/*!
 * \brief Retrieve the chunk-version for a file/chunkId
 * \param[in] file	file id for the file
 * \param[in] chunkId	chunkId of interest
 * \param[out] chunkVersion  the version # of chunkId if such
 * a chunkId exists; 0 otherwise
 * \return 	status code
*/
int
Tree::getChunkVersion(fid_t file, chunkId_t chunkId, seq_t *chunkVersion)
{
	vector <MetaChunkInfo *> v;
	vector <MetaChunkInfo *>::iterator i;
	MetaChunkInfo *m;

	*chunkVersion = 0;
	getalloc(file, v);
	i = find_if(v.begin(), v.end(), ChunkIdMatch(chunkId));
	if (i == v.end())
		return -ENOENT;
	m = *i;
	*chunkVersion = m->chunkVersion;
	return 0;
}

/*!
 * \brief allocate a chunk id for a file.
 * \param[in] file	file id for the file
 * \param[in] offset	offset in the file
 * \param[out] chunkId	chunkId that is (pre) allocated.  Allocation
 * is a two-step process: we grab a chunkId and then try to place the
 * chunk on a chunkserver; only when placement succeeds can the
 * chunkId be assigned to the file.  This function does the part of
 * grabbing the chunkId.
 * \param[out] chunkVersion  The version # assigned to the chunk
 * \return		status code
 */
int
Tree::allocateChunkId(fid_t file, chunkOff_t offset, chunkId_t *chunkId,
			seq_t *chunkVersion, int16_t *numReplicas)
{
	MetaFattr *fa = getFattr(file);
	if (fa == NULL)
		return -ENOENT;

	if (numReplicas != NULL)
		*numReplicas = fa->numReplicas;

	// Allocation information is stored for offset's in the file that
	// correspond to chunk boundaries.  This simplifies finding
	// allocation information as we need to look for chunk
	// starting locations only.
	assert(offset % CHUNKSIZE == 0);
	chunkOff_t boundary = chunkStartOffset(offset);
	const Key ckey(KFS_CHUNKINFO, file, boundary);
	Node *l = findLeaf(ckey);

	// check if an id has already been assigned to this offset
	if (l != NULL) {
		MetaChunkInfo *c = l->extractMeta<MetaChunkInfo>(ckey);
		*chunkId = c->chunkId;
		*chunkVersion = c->chunkVersion;
		return -EEXIST;
	}

	// during replay chunkId will be non-zero.  In such cases,
	// don't do new allocation.
	if (*chunkId == 0) {
		*chunkId = chunkID.genid();
		*chunkVersion = chunkVersionInc;
	}
	return 0;
}

/*!
 * \brief update the metatree to link an allocated a chunk id with
 * its associated file.
 * \param[in] file	file id for the file
 * \param[in] offset	offset in the file
 * \param[in] chunkId	chunkId that is (pre) allocated.  Allocation
 * is a two-step process: we grab a chunkId and then try to place the
 * chunk on a chunkserver; only when placement succeeds can the
 * chunkId be assigned to the file.  This function does the part of
 * assinging the chunkId to the file.
 * \param[in] chunkVersion chunkVersion that is (pre) assigned.
 * \return		status code
 */
int
Tree::assignChunkId(fid_t file, chunkOff_t offset,
		    chunkId_t chunkId, seq_t chunkVersion)
{
	chunkOff_t boundary = chunkStartOffset(offset);
	MetaFattr *fa = getFattr(file);
	if (fa == NULL)
		return -ENOENT;

	// check if an id has already been assigned to this chunk
	const Key ckey(KFS_CHUNKINFO, file, boundary);
	Node *l = findLeaf(ckey);
	if (l != NULL) {
		MetaChunkInfo *c = l->extractMeta<MetaChunkInfo>(ckey);
		chunkId = c->chunkId;
		if (c->chunkVersion == chunkVersion)
			return -EEXIST;
		c->chunkVersion = chunkVersion;
		return 0;
	}

	MetaChunkInfo *m = new MetaChunkInfo(file, offset, 
					chunkId, chunkVersion);
	if (insert(m)) {
		// insert failed
		delete m;
		panic("assignChunk", false);
	}

	// insert succeeded; so, bump the chunkcount.
	fa->chunkcount++;

	gettimeofday(&fa->mtime, NULL);
	return 0;
}

static bool
ChunkInfo_compare(MetaChunkInfo *first, MetaChunkInfo *second)
{
	return first->offset < second->offset;
}

int
Tree::truncate(fid_t file, chunkOff_t offset, chunkOff_t *allocOffset)
{
	MetaFattr *fa = getFattr(file);

	if (fa == NULL)
		return -ENOENT;
	if (fa->type != KFS_FILE)
		return -EISDIR;

	vector <MetaChunkInfo *> chunkInfo;
	vector <MetaChunkInfo *>::iterator m;

	getalloc(fa->id(), chunkInfo);
	assert(fa->chunkcount == (long long)chunkInfo.size());

	// compute the starting offset for what will be the
	// "last" chunk for the file
	chunkOff_t lastChunkStartOffset = chunkStartOffset(offset);

	MetaChunkInfo last(fa->id(), lastChunkStartOffset, 0, 0);

	m = lower_bound(chunkInfo.begin(), chunkInfo.end(),
			&last, ChunkInfo_compare);
        
        //
        // If there is no chunk corresponding to the offset to which
        // the file should be truncated, allocate one at that point.
        // This can happen due to the following cases:
        // 1. The offset to truncate to exceeds the last chunk of
        // the file.
        // 2. There is a hole in the file and the offset to truncate
        // to corresponds to the hole.
        //
	if ((m == chunkInfo.end()) || 
	    ((*m)->offset != lastChunkStartOffset)) {
		// Allocate a chunk at this offset
		*allocOffset = lastChunkStartOffset;
		return 1;
	}

	if ((*m)->offset <= offset) {
		// truncate the last chunk so that the file
		// has the desired size.
		(*m)->TruncateChunk(offset - (*m)->offset);
		++m;
	}

	// delete everything past the last chunk
	while (m != chunkInfo.end()) {
		(*m)->DeleteChunk();
		++m;
		fa->chunkcount--;
	}

	gettimeofday(&fa->mtime, NULL);
	return 0;
}

/*!
 * \brief check whether one directory is a descendant of another
 * \param[in] src	file ID of possible ancestor
 * \param[in] dst	file ID of possible descendant
 *
 * Check dst and each of its ancestors to see whether src is
 * among them; used to avoid making a directory into its own
 * child via rename.
 */
bool
Tree::is_descendant(fid_t src, fid_t dst)
{
	while (src != dst && dst != ROOTFID) {
		MetaFattr *dotdot = lookup(dst, "..");
		dst = dotdot->id();
	}

	return (src == dst);
}

/*!
 * \brief rename a file or directory
 * \param[in]	parent	file id of parent directory
 * \param[in]	oldname	the file's current name
 * \param[in]	newname	the new name for the file
 * \param[in]	overwrite when set, overwrite the dest if it exists
 * \return		status code
 */
int
Tree::rename(fid_t parent, const string &oldname, string &newname, 
		bool overwrite)
{
	int status;
	MetaDentry *src = getDentry(parent, oldname);
	if (src == NULL)
		return -ENOENT;

	fid_t ddir;
	string dname;
	string::size_type rslash = newname.rfind('/');
	if (rslash == string::npos) {
		ddir = parent;
		dname = newname;
	} else {
		MetaFattr *ddfattr = lookupPath(
				parent, newname.substr(0, rslash));
		if (ddfattr == NULL)
			return -ENOENT;
		else if (ddfattr->type != KFS_DIR)
			return -ENOTDIR;
		else
			ddir = ddfattr->id();
		dname = newname.substr(rslash + 1);
	}

	if (!legalname(dname))
		return -EINVAL;

	if (ddir == parent && dname == oldname)
		return 0;

	MetaFattr *sfattr = lookup(parent, oldname);
	MetaFattr *dfattr = lookup(ddir, dname);
	bool dexists = (dfattr != NULL);
	FileType t = sfattr->type;
	
	if ((!overwrite) && dexists)
		return -EEXIST;

	if (dexists && t != dfattr->type)
		return (t == KFS_DIR) ? -ENOTDIR : -EISDIR;

	if (dexists && t == KFS_DIR && !emptydir(dfattr->id()))
		return -ENOTEMPTY;

	if (t == KFS_DIR && is_descendant(sfattr->id(), ddir))
		return -EINVAL;

	if (dexists) {
		status = (t == KFS_DIR) ?
			rmdir(ddir, dname) : remove(ddir, dname);
		if (status != 0)
			return status;
	}

	fid_t srcFid = src->id();

	if (t == KFS_DIR) {
		// get rid of the linkage of the "old" ..
		unlink(srcFid, "..", sfattr, true);
	}

	status = del(src);
	assert(status == 0);
	MetaDentry *newSrc = new MetaDentry(ddir, dname, srcFid);
	status = insert(newSrc);
	assert(status == 0);
	if (t == KFS_DIR) {
		// create a new linkage for ..
		status = link(srcFid, "..", KFS_DIR, ddir, 1);
		assert(status == 0);
	}
	return 0;
}


/*!
 * \brief Change the degree of replication for a file.
 * \param[in] dir	file id of the file
 * \param[in] numReplicas	desired degree of replication
 * \return		status code (-errno on failure)
 */
int
Tree::changeFileReplication(fid_t fid, int16_t numReplicas)
{
	MetaFattr *fa = getFattr(fid);
        vector<MetaChunkInfo*> chunkInfo;

	if (fa == NULL)
		return -ENOENT;

	fa->setReplication(numReplicas);

        getalloc(fid, chunkInfo);

        for (vector<ChunkLayoutInfo>::size_type i = 0; i < chunkInfo.size(); ++i) {
		gLayoutManager.ChangeChunkReplication(chunkInfo[i]->chunkId);
	}
	return 0;
}

/*!
 * \brief  A file that has to be removed is currently busy.  So, rename the
 * file to the dumpster and we'll clean it up later.
 * \param[in] dir	file id of the parent directory
 * \param[in] fname	file name
 * \return		status code (zero on success)
 */
int
Tree::moveToDumpster(fid_t dir, const string &fname)
{
	static uint64_t counter = 1;
	string tempname = "/" + DUMPSTERDIR + "/";
	MetaFattr *fa = lookup(ROOTFID, DUMPSTERDIR);

	if (fa == NULL) {
		// Someone nuked the dumpster
		KFS_LOG_DEBUG("No dumpster dir...recreating...");
		makeDumpsterDir();
		fa = lookup(ROOTFID, DUMPSTERDIR);
		if (fa == NULL) {
			assert(!"No dumpster");
			KFS_LOG_INFO("Unable to create dumpster dir to remove %s",
					fname.c_str());
			return -1;
		}
	}

	// can't move something in the dumpster back to dumpster
	if (fa->id() == dir)
		return -EEXIST;

	// generate a unique name
	tempname += fname + boost::lexical_cast<string>(counter);

	counter++;
	return rename(dir, fname, tempname, true);
}

class RemoveDumpsterEntry {
	fid_t dir;
public:
	RemoveDumpsterEntry(fid_t d) : dir(d) { }
	void operator() (MetaDentry *e) {
		metatree.remove(dir, e->getName());
	}
};

/*!
 * \brief Periodically, cleanup the dumpster and reclaim space.  If
 * the lease issued on a file has expired, then the file can be nuked.
 */
void
Tree::cleanupDumpster()
{
	MetaFattr *fa = lookup(ROOTFID, DUMPSTERDIR);

	if (fa == NULL) {
		// Someone nuked the dumpster
		KFS_LOG_DEBUG("No dumpster...recreating...");
		makeDumpsterDir();
	}
		
	fid_t dir = fa->id();

	vector <MetaDentry *> v;
	readdir(dir, v);

	for_each(v.begin(), v.end(), RemoveDumpsterEntry(dir));
}
