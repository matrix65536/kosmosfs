//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: KfsAccessJni.cc $
//
// Created 2007/08/24
// Author: Sriram Rao (Kosmix Corp.) 
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
// \brief JNI code in C++ world for accesing KFS Client.
//
//----------------------------------------------------------------------------

#include <jni.h>
#include <string>
#include <cstddef>
#include <iostream>
#include <vector>
using std::vector;
using std::string;
using std::cout;
using std::endl;

#include "libkfsClient/KfsClient.h"
using namespace KFS;

extern "C" {
    jint Java_org_kosmos_access_KfsAccess_initF(
        JNIEnv *jenv, jclass jcls, jstring jpath);

    jint Java_org_kosmos_access_KfsAccess_initS(
        JNIEnv *jenv, jclass jcls, jstring jmetaServerHost, jint metaServerPort);
    
    jint Java_org_kosmos_access_KfsAccess_cd(
        JNIEnv *jenv, jclass jcls, jstring jpath);

    jint Java_org_kosmos_access_KfsAccess_mkdirs(
        JNIEnv *jenv, jclass jcls, jstring jpath);

    jint Java_org_kosmos_access_KfsAccess_rmdir(
        JNIEnv *jenv, jclass jcls, jstring jpath);

    jobjectArray Java_org_kosmos_access_KfsAccess_readdir(
        JNIEnv *jenv, jclass jcls, jstring jpath);

    jint Java_org_kosmos_access_KfsAccess_remove(
        JNIEnv *jenv, jclass jcls, jstring jpath);

    jint Java_org_kosmos_access_KfsAccess_rename(
        JNIEnv *jenv, jclass jcls, jstring joldpath, jstring jnewpath,
        jboolean joverwrite);

    jint Java_org_kosmos_access_KfsAccess_exists(
        JNIEnv *jenv, jclass jcls, jstring jpath);

    jint Java_org_kosmos_access_KfsAccess_isFile(
        JNIEnv *jenv, jclass jcls, jstring jpath);

    jint Java_org_kosmos_access_KfsAccess_isDirectory(
        JNIEnv *jenv, jclass jcls, jstring jpath);

    jlong Java_org_kosmos_access_KfsAccess_filesize(
        JNIEnv *jenv, jclass jcls, jstring jpath);

    jobjectArray Java_org_kosmos_access_KfsAccess_getDataLocation(
        JNIEnv *jenv, jclass jcls, jstring jpath, jlong jstart, jlong jlen);

    jint Java_org_kosmos_access_KfsAccess_open(
        JNIEnv *jenv, jclass jcls, jstring jpath, jstring jmode, jint jnumReplicas);

    jint Java_org_kosmos_access_KfsAccess_create(
        JNIEnv *jenv, jclass jcls, jstring jpath, jint jnumReplicas);


    /* Input channel methods */
    jint Java_org_kosmos_access_KfsInputChannel_read(
        JNIEnv *jenv, jclass jcls, jint jfd, jobject buf, jint begin, jint end);

    jint Java_org_kosmos_access_KfsInputChannel_seek(
        JNIEnv *jenv, jclass jcls, jint jfd, jlong joffset);

    jint Java_org_kosmos_access_KfsInputChannel_tell(
        JNIEnv *jenv, jclass jcls, jint jfd);

    jint Java_org_kosmos_access_KfsInputChannel_close(
        JNIEnv *jenv, jclass jcls, jint jfd);

    /* Output channel methods */
    jint Java_org_kosmos_access_KfsOutputChannel_write(
        JNIEnv *jenv, jclass jcls, jint jfd, jobject buf, jint begin, jint end);

    jint Java_org_kosmos_access_KfsOutputChannel_sync(
        JNIEnv *jenv, jclass jcls, jint jfd);

    jint Java_org_kosmos_access_KfsOutputChannel_seek(
        JNIEnv *jenv, jclass jcls, jint jfd, jlong joffset);

    jint Java_org_kosmos_access_KfsOutputChannel_tell(
        JNIEnv *jenv, jclass jcls, jint jfd);

    jint Java_org_kosmos_access_KfsOutputChannel_close(
        JNIEnv *jenv, jclass jcls, jint jfd);

}

namespace
{
    inline void setStr(string & dst, JNIEnv * jenv, jstring src)
    {
        char const * s = jenv->GetStringUTFChars(src, 0);
        dst.assign(s);
        jenv->ReleaseStringUTFChars(src, s);
    }
}

jint Java_org_kosmos_access_KfsAccess_initF(JNIEnv *jenv, jclass jcls, jstring jpath)
{
    KfsClient *clnt = KfsClient::Instance();

    string path;
    setStr(path, jenv, jpath);
    return clnt->Init(path.c_str());
}

jint Java_org_kosmos_access_KfsAccess_initS(JNIEnv *jenv, jclass jcls, 
                                            jstring jmetaServerHost, jint metaServerPort)
{
    KfsClient *clnt = KfsClient::Instance();

    string path;
    setStr(path, jenv, jmetaServerHost);
    return clnt->Init(path, metaServerPort);
}
    
jint Java_org_kosmos_access_KfsAccess_cd(JNIEnv *jenv, jclass jcls, jstring jpath)
{
    KfsClient *clnt = KfsClient::Instance();

    string path;
    setStr(path, jenv, jpath);
    return clnt->Cd(path.c_str());
}

jint Java_org_kosmos_access_KfsAccess_mkdirs(JNIEnv *jenv, jclass jcls, jstring jpath)
{
    KfsClient *clnt = KfsClient::Instance();

    string path;
    setStr(path, jenv, jpath);
    return clnt->Mkdirs(path.c_str());
}

jint Java_org_kosmos_access_KfsAccess_rmdir(JNIEnv *jenv, jclass jcls, jstring jpath)
{
    KfsClient *clnt = KfsClient::Instance();

    string path;
    setStr(path, jenv, jpath);
    return clnt->Rmdir(path.c_str());
}

jobjectArray Java_org_kosmos_access_KfsAccess_readdir(JNIEnv *jenv, jclass jcls, jstring jpath)
{
    KfsClient *clnt = KfsClient::Instance();

    string path;
    vector<string> entries;
    int res;
    jstring s;
    jobjectArray jentries;

    setStr(path, jenv, jpath);

    res = clnt->Readdir(path.c_str(), entries);
    if ((res < 0) || (entries.size() == 0))
        return NULL;

    jclass jstrClass = jenv->FindClass("Ljava/lang/String;");

    jentries = jenv->NewObjectArray(entries.size(), jstrClass, NULL);
    for (vector<string>::size_type i = 0; i < entries.size(); i++) {
        s = jenv->NewStringUTF(entries[i].c_str());
        // std::cout << "Setting: " << i << ' ' << entries[i] << std::endl;
        jenv->SetObjectArrayElement(jentries, i, s);
        jenv->DeleteLocalRef(s);
    }

    return jentries;
}

jint Java_org_kosmos_access_KfsAccess_open(JNIEnv *jenv, jclass jcls, 
                                           jstring jpath, jstring jmode,
                                           jint jnumReplicas)
{
    KfsClient *clnt = KfsClient::Instance();

    string path, mode;
    int openMode = 0;

    setStr(path, jenv, jpath);
    setStr(mode, jenv, jmode);

    if (mode == "r")
        openMode = O_RDONLY;
    else if (mode == "w")
        openMode = O_WRONLY|O_CREAT;
    else if (mode == "rw")
        openMode = O_RDWR;
    else if (mode == "a")
        openMode = O_WRONLY | O_APPEND;

    return clnt->Open(path.c_str(), openMode, jnumReplicas);
}

jint Java_org_kosmos_access_KfsInputChannel_close(JNIEnv *jenv, jclass jcls, jint jfd)
{
    KfsClient *clnt = KfsClient::Instance();
    
    return clnt->Close(jfd);
}

jint Java_org_kosmos_access_KfsOutputChannel_close(JNIEnv *jenv, jclass jcls, jint jfd)
{
    KfsClient *clnt = KfsClient::Instance();
    
    return clnt->Close(jfd);
}

jint Java_org_kosmos_access_KfsAccess_create(JNIEnv *jenv, jclass jcls, 
                                             jstring jpath, jint jnumReplicas)
{
    KfsClient *clnt = KfsClient::Instance();

    string path;
    setStr(path, jenv, jpath);
    return clnt->Create(path.c_str(), jnumReplicas);
}

jint Java_org_kosmos_access_KfsAccess_remove(JNIEnv *jenv, jclass jcls, jstring jpath)
{
    KfsClient *clnt = KfsClient::Instance();

    string path;
    setStr(path, jenv, jpath);
    return clnt->Remove(path.c_str());
}

jint Java_org_kosmos_access_KfsAccess_rename(JNIEnv *jenv, jclass jcls, 
                                             jstring joldpath, jstring jnewpath,
                                             jboolean joverwrite)
{
    KfsClient *clnt = KfsClient::Instance();

    string opath, npath;
    setStr(opath, jenv, joldpath);
    setStr(npath, jenv, jnewpath);

    return clnt->Rename(opath.c_str(), npath.c_str(), joverwrite);
}

jint Java_org_kosmos_access_KfsOutputChannel_sync(JNIEnv *jenv, jclass jcls, jint jfd)
{
    KfsClient *clnt = KfsClient::Instance();
    
    return clnt->Sync(jfd);
}

jint Java_org_kosmos_access_KfsInputChannel_seek(JNIEnv *jenv, jclass jcls, jint jfd, jlong joffset)
{
    KfsClient *clnt = KfsClient::Instance();
    
    return clnt->Seek(jfd, joffset);
}

jint Java_org_kosmos_access_KfsInputChannel_tell(JNIEnv *jenv, jclass jcls, jint jfd)
{
    KfsClient *clnt = KfsClient::Instance();
    
    return clnt->Tell(jfd);
}

jint Java_org_kosmos_access_KfsOutputChannel_seek(JNIEnv *jenv, jclass jcls, jint jfd, jlong joffset)
{
    KfsClient *clnt = KfsClient::Instance();
    
    return clnt->Seek(jfd, joffset);
}

jint Java_org_kosmos_access_KfsOutputChannel_tell(JNIEnv *jenv, jclass jcls, jint jfd)
{
    KfsClient *clnt = KfsClient::Instance();
    
    return clnt->Tell(jfd);
}

jint Java_org_kosmos_access_KfsAccess_exists(JNIEnv *jenv, jclass jcls, jstring jpath)
{
    KfsClient *clnt = KfsClient::Instance();

    string path;
    setStr(path, jenv, jpath);
    
    if (clnt->Exists(path.c_str()))
        return 1;
    
    return 0;
}

jint Java_org_kosmos_access_KfsAccess_isFile(JNIEnv *jenv, jclass jcls, jstring jpath)
{
    KfsClient *clnt = KfsClient::Instance();

    string path;
    setStr(path, jenv, jpath);
    
    if (clnt->IsFile(path.c_str()))
        return 1;
    return 0;
}

jint Java_org_kosmos_access_KfsAccess_isDirectory(JNIEnv *jenv, jclass jcls, jstring jpath)
{
    KfsClient *clnt = KfsClient::Instance();

    string path;
    setStr(path, jenv, jpath);
    
    if (clnt->IsDirectory(path.c_str()))
        return 1;
    return 0;
}

jlong Java_org_kosmos_access_KfsAccess_filesize(JNIEnv *jenv, jclass jcls, jstring jpath)
{
    KfsClient *clnt = KfsClient::Instance();

    struct stat result;
    string path;
    setStr(path, jenv, jpath);
    
    if (clnt->Stat(path.c_str(), result) != 0)
        return -1;
    
    return result.st_size;
}

jobjectArray Java_org_kosmos_access_KfsAccess_getDataLocation(JNIEnv *jenv, jclass jcls, jstring jpath,
                                                              jlong jstart, jlong jlen)
{
    KfsClient *clnt = KfsClient::Instance();

    string path;
    // for each block, there could be multiple locations due to replication; return them all here
    vector< vector<string> > entries;
    int res;
    jstring s;
    jobjectArray jentries;

    setStr(path, jenv, jpath);

    res = clnt->GetDataLocation(path.c_str(), jstart, jlen, entries);
    if ((res < 0) || (entries.size() == 0))
        return NULL;

    jclass jstrArrClass = jenv->FindClass("[Ljava/lang/String;");
    jclass jstrClass = jenv->FindClass("Ljava/lang/String;");

    // For each block, return its location(s)
    jentries = jenv->NewObjectArray(entries.size(), jstrArrClass, NULL);

    for (vector<string>::size_type i = 0; i < entries.size(); i++) {
        jobjectArray jlocs = jenv->NewObjectArray(entries[i].size(), jstrClass, NULL);
        for (vector<string>::size_type j = 0; j < entries[i].size(); j++) {
            s = jenv->NewStringUTF(entries[i][j].c_str());
            jenv->SetObjectArrayElement(jlocs, j, s);
            jenv->DeleteLocalRef(s);
        }
        jenv->SetObjectArrayElement(jentries, i, jlocs);
        jenv->DeleteLocalRef(jlocs);
    }

    return jentries;
}

jint Java_org_kosmos_access_KfsInputChannel_read(JNIEnv *jenv, jclass jcls, jint jfd, 
                                                 jobject buf, jint begin, jint end) 
{
    KfsClient *clnt = KfsClient::Instance();

    if(!buf)
        return 0;

    void * addr = jenv->GetDirectBufferAddress(buf);
    jlong cap = jenv->GetDirectBufferCapacity(buf);

    if(!addr || cap < 0)
        return 0;
    if(begin < 0 || end > cap || begin > end)
        return 0;

    addr = (void *)(uintptr_t(addr) + begin);
        
    ssize_t sz = clnt->Read((int) jfd, (char *) addr, (size_t) (end - begin));
    return (jint)sz;
}


jint Java_org_kosmos_access_KfsOutputChannel_write(JNIEnv *jenv, jclass jcls, jint jfd, 
                                                   jobject buf, jint begin, jint end) 
{
    KfsClient *clnt = KfsClient::Instance();

    if(!buf)
        return 0;

    void * addr = jenv->GetDirectBufferAddress(buf);
    jlong cap = jenv->GetDirectBufferCapacity(buf);

    if(!addr || cap < 0)
        return 0;
    if(begin < 0 || end > cap || begin > end)
        return 0;

    addr = (void *)(uintptr_t(addr) + begin);
        
    ssize_t sz = clnt->Write((int) jfd, (const char *) addr, (size_t) (end - begin));
    return (jint)sz;
}
