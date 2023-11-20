//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/08/01
// Author: Blake Lewis (Kosmix Corp.)
//
// Copyright 2008-2012,2016 Quantcast Corporation. All rights reserved.
// Copyright 2006-2008 Kosmix Corp.
//
// This file is part of Kosmos File System (KFS).
//
// Licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// \brief Glue code for Python apps to access QFS.
//
//  Note: The Python Extension Module is in experimental stage. Please use it
//        with caution.
//----------------------------------------------------------------------------

#define PY_SSIZE_T_CLEAN

#include "Python.h"
#include "structmember.h"
#include "libclient/KfsClient.h"
#include <cstring>
#include <cerrno>
#include <string>
#include <vector>

#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>

using std::string;
using std::vector;

using KFS::KfsClient;
using KFS::chunkOff_t;
using KFS::KfsFileAttr;
using KFS::ErrorCodeToStr;

struct qfs_Client {
    PyObject_HEAD
    PyObject *qfshost;        // QFS metaserver host
    int qfsport;              // QSF metaserver port
    PyObject *cwd;            // Current directory
    KfsClient *client;        // The client itself
};

static PyObject *Client_new(PyTypeObject *type, PyObject *args, PyObject *kwds);
static int Client_init(PyObject *pself, PyObject *args, PyObject *kwds);
static void Client_dealloc(PyObject *pself);

static PyObject *Client_repr(PyObject *pself);
static PyObject *qfs_isdir(PyObject *pself, PyObject *args);
static PyObject *qfs_isfile(PyObject *pself, PyObject *args);
static PyObject *qfs_mkdir(PyObject *pself, PyObject *args);
static PyObject *qfs_mkdirs(PyObject *pself, PyObject *args);
static PyObject *qfs_rmdir(PyObject *pself, PyObject *args);
static PyObject *qfs_rmdirs(PyObject *pself, PyObject *args);
static PyObject *qfs_readdir(PyObject *pself, PyObject *args);
static PyObject *qfs_readdirplus(PyObject *pself, PyObject *args);
static PyObject *qfs_create(PyObject *pself, PyObject *args);
static PyObject *qfs_stat(PyObject *pself, PyObject *args);
static PyObject *qfs_fullstat(PyObject *pself, PyObject *args);
static PyObject *qfs_getNumChunks(PyObject *pself, PyObject *args);
static PyObject *qfs_getChunkSize(PyObject *pself, PyObject *args);
static PyObject *qfs_remove(PyObject *pself, PyObject *args);
static PyObject *qfs_rename(PyObject *pself, PyObject *args);
static PyObject *qfs_coalesceblocks(PyObject *pself, PyObject *args);
static PyObject *qfs_open(PyObject *pself, PyObject *args);
static PyObject *qfs_cd(PyObject *pself, PyObject *args);
static PyObject *qfs_log_level(PyObject *pself, PyObject *args);

inline static void SetPyIoError(int64_t err)
{
    const string s = ErrorCodeToStr((int)err);
    PyErr_SetString(PyExc_IOError, s.c_str());
}

static PyMemberDef Client_members[] = {
    { "qfshost",    T_OBJECT, offsetof(qfs_Client, qfshost),  READONLY, "QFS metaserver hostname" },
    { "qfsport",    T_INT,    offsetof(qfs_Client, qfsport),  READONLY, "QFS metaserver port"     },
    { "cwd",        T_OBJECT, offsetof(qfs_Client, cwd),      READONLY, "current directory"       },
    { NULL }
};

static PyMethodDef Client_methods[] = {
    { "mkdir",            qfs_mkdir,          METH_VARARGS, "Create directory."},
    { "mkdirs",           qfs_mkdirs,         METH_VARARGS, "Create directory tree."},
    { "isdir",            qfs_isdir,          METH_VARARGS, "Check if a path is a directory."},
    { "rmdir",            qfs_rmdir,          METH_VARARGS, "Remove directory."},
    { "rmdirs",           qfs_rmdirs,         METH_VARARGS, "Remove directory tree."},
    { "readdir",          qfs_readdir,        METH_VARARGS, "Read directory." },
    { "readdirplus",      qfs_readdirplus,    METH_VARARGS, "Read directory with attributes." },
    { "stat",             qfs_stat,           METH_VARARGS, "Stat file." },
    { "fullstat",         qfs_fullstat,       METH_VARARGS, "Stat file for QFS attributes." },
    { "getNumChunks",     qfs_getNumChunks,   METH_VARARGS, "Get # of chunks in a file." },
    { "getChunkSize",     qfs_getChunkSize,   METH_VARARGS, "Get default chunksize for a file." },
    { "create",           qfs_create,         METH_VARARGS, "Create file." },
    { "remove",           qfs_remove,         METH_VARARGS, "Remove file." },
    { "rename",           qfs_rename,         METH_VARARGS, "Rename file or directory." },
    { "coalesce_blocks",  qfs_coalesceblocks, METH_VARARGS, "Coalesce blocks from src->dest." },
    { "open",             qfs_open,           METH_VARARGS, "Open file." },
    { "isfile",           qfs_isfile,         METH_VARARGS, "Check if a path is a file."},
    { "cd",               qfs_cd,             METH_VARARGS, "Change directory." },
    { "log_level",        qfs_log_level,      METH_VARARGS, "Set log4cpp log level." },
    { NULL, NULL}
};

PyDoc_STRVAR(Client_doc,
"A qfs.client object is an instance of the QFS client library\n"
"that sends RPC's to the QFS metadata and chunk servers in order\n"
"to perform file system operations.  In addition, its 'open' method\n"
"creates qfs.file objects that represent files in QFS.\n\n"
"To create a client, you must supply a 'properties file' which\n"
"defines the hostname and port number for the metaserver, e.g.\n\n"
"\tmy_client = qfs.client('QfsTester.properties')\n\n"
"Methods:\n"
"\tmkdir(path) -- create a directory\n"
"\tmkdirs(path) -- create a directory tree\n"
"\trmdir(path) -- remove a directory (path should be an empty dir)\n"
"\trmdirs(path) -- remove a directory tree\n"
"\treaddir(path) -- return a tuple of directory contents\n"
"\treaddirplus(path) -- directory entries plus attributes\n"
"\tisdir(path) -- return TRUE if path is a directory\n"
"\tisfile(path) -- return TRUE if path is a file\n"
"\tstat(path)   --  file attributes, compatible with os.stat\n"
"\tfullstat(path)   --  file attributes, including QFS attributes\n"
"\tgetNumChunks(path)   --  return the # of chunks in a file\n"
"\tgetChunkSize(path)   --  return the default size of chunks in a file\n"
"\tcreate(path, numReplicas=3) -- create a file and return a qfs.file object for it\n"
"\tremove(path) -- remove a file\n"
"\tcoalesceblocks(src, dst) -- append blocks from src->dest\n"
"\topen(path[, mode, encoding, errors]) -- open a file and return an object for it\n"
"\tcd(path)     -- change current directory\n"
"\tlog_level(level)     -- change the message log level\n"
"\n\nData:\n"
"\tproperties   -- the name of the properties file\n"
"\tcwd          -- the current directory (for relative paths)\n");

static PyTypeObject qfs_ClientType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "qfs.client",         // tp_name
    sizeof (qfs_Client),  // tp_basicsize
    0,                    // tp_itemsize
    Client_dealloc,       // tp_dealloc
    0,                    // tp_vectorcall_offset
    0,                    // tp_getattr
    0,                    // tp_setattr
    0,                    // tp_as_async
    Client_repr,          // tp_repr
    0,                    // tp_as_number
    0,                    // tp_as_sequence
    0,                    // tp_as_mapping
    0,                    // tp_hash
    0,                    // tp_call
    0,                    // tp_str
    0,                    // tp_getattro
    0,                    // tp_setattro
    0,                    // tp_as_buffer
    Py_TPFLAGS_DEFAULT,   // tp_flags
    Client_doc,           // tp_doc
    0,                    // tp_traverse
    0,                    // tp_clear
    0,                    // tp_richcompare
    0,                    // tp_weaklistoffest
    0,                    // tp_iter
    0,                    // tp_iternext
    Client_methods,       // tp_methods
    Client_members,       // tp_members
    0,                    // tp_getset
    0,                    // tp_base
    0,                    // tp_dict
    0,                    // tp_descr_get
    0,                    // tp_descr_set
    0,                    // tp_dictoffset
    Client_init,          // tp_init
    0,                    // tp_alloc
    Client_new            // tp_new
};


struct qfs_File {
    PyObject_HEAD
    PyObject *name;       // File name
    PyObject *mode;       // Access mode
    PyObject *pclient;    // Python object for QFS client
    int fd;               // File descriptor
    bool binary;          // Binary mode
    char* encoding;
    char* errors;
};

static PyObject *
File_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    static PyObject *noname = NULL;
    if (noname == NULL) {
        noname = PyUnicode_FromString("<uninitialized file>");
        if (noname == NULL)
            return NULL;
    }
    qfs_File *self = (qfs_File *)type->tp_alloc(type, 0);
    if (self != NULL) {
        Py_INCREF(noname);
        self->name = noname;
        Py_INCREF(noname);
        self->mode = noname;
        Py_INCREF(noname);
        self->pclient = noname;
        self->fd = -1;
        self->binary = false;
        self->encoding = NULL;
        self->errors = NULL;
    }
    return (PyObject *)self;
}

static void
File_dealloc(PyObject *pself)
{
    qfs_File *self = (qfs_File *)pself;
    qfs_Client *cl = (qfs_Client *)self->pclient;
    if (self->fd != -1)
        cl->client->Close(self->fd);
    free(self->encoding);
    free(self->errors);
    Py_DECREF(self->name);
    Py_DECREF(self->mode);
    Py_DECREF(self->pclient);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static int
modeflag(const char *modestr, bool *binary)
{
    // convert mode string to flag
    int mode = 0;
    bool read = false;
    bool write = false;
    int tb = 0;
    const char* p;

    *binary = false;
    for (p = modestr; *p != 0; p += 1) {
        switch (*p & 0xFF) {
            case 'r': read = true; break;
            case 'w': write = true; break;
            case 'x': mode |= O_EXCL; break;
            case 'a': write = true; mode |= O_APPEND; break;
            case 'b':
                if (tb == 't') return -1;
                tb = 'b'; *binary = true; break;
            case 't':
                if (tb == 'b') return -1;
                tb = 't'; *binary = false; break;
            case '+': read = true; write = true; break;
            default: return -1;
        }
    }

    return mode | (write ? (read ? O_RDWR : O_WRONLY) : O_RDONLY);
}

static int
set_file_members(
        qfs_File *self, const char *path,
        const char *modestr, qfs_Client *client, int fd,
        const char* encoding, const char* errors)
{
    int mode;
    bool binary;

    // convert mode string to flag
    mode = modeflag(modestr, &binary);
    if (mode == -1)
        return -1;

    // open the file if necessary
    if (fd < 0)
        fd = client->client->Open(path, mode);

    if (fd < 0) {
        SetPyIoError(fd);
        return -1;
    }

    // set all of the fields in the qfs_File structure
    Py_DECREF(self->name);
    self->name = PyUnicode_FromString(path);
    Py_DECREF(self->mode);
    self->mode = PyUnicode_FromString(modestr);
    PyObject *pclient = (PyObject *)client;
    Py_INCREF(pclient);
    Py_DECREF(self->pclient);
    self->pclient = pclient;
    self->fd = fd;
    self->binary = binary;
    free(self->encoding);
    self->encoding = NULL;
    free(self->errors);
    self->errors = NULL;
    if (encoding) {
        self->encoding = strdup(encoding);
    }
    if (errors) {
        self->errors = strdup(errors);
    }


    return 0;
}

static int
File_init(PyObject *pself, PyObject *args, PyObject *kwds)
{
    qfs_File *self = (qfs_File *)pself;
    const char *nm, *md = "r";
    PyObject *cl = NULL;

    static char *kwlist[] = {
      (char*)"client", (char*)"name", (char*)"mode", NULL
    };

    int ok = !PyArg_ParseTupleAndKeywords(
            args, kwds, "O!s|s", kwlist, &qfs_ClientType,
            &cl, &nm, &md);
    if (!ok)
        return -1;

    return set_file_members(self, nm, md, (qfs_Client *)cl, -1, NULL, NULL);
}

static PyObject *
File_repr(PyObject *pself)
{
    qfs_File *self = (qfs_File *)pself;
    return PyUnicode_FromFormat("qfs.file<%s, %s, %d>",
            PyUnicode_AsUTF8(self->name),
            PyUnicode_AsUTF8(self->mode),
            self->fd);
}

static PyObject *
qfs_reopen(PyObject *pself, PyObject *args)
{
    qfs_File *self = (qfs_File *)pself;
    qfs_Client *cl = (qfs_Client *)self->pclient;
    const char *modestr = PyUnicode_AsUTF8(self->mode);
    bool binary;

    if (self->fd >= 0) {
        int ret = cl->client->Close(self->fd);
        self->fd = -1;
        if (ret < 0) {
            SetPyIoError(ret);
            return NULL;
        }
    }

    if (!PyArg_ParseTuple(args, "|s", &modestr))
        return NULL;

    int mode = modeflag(modestr, &binary);
    if (mode == -1)
        return NULL;

    int fd = cl->client->Open(PyUnicode_AsUTF8(self->name), mode);
    if (fd == -1)
        return NULL;

    self->fd = fd;
    self->mode = PyUnicode_FromString(modestr);
    self->binary = binary;
    Py_RETURN_NONE;
}

static PyObject *
qfs_close(PyObject *pself, PyObject *args)
{
    qfs_File *self = (qfs_File *)pself;
    qfs_Client *cl = (qfs_Client *)self->pclient;
    if (self->fd != -1) {
        int ret = cl->client->Close(self->fd);
        self->fd = -1;
        if (ret < 0) {
            SetPyIoError(ret);
            return NULL;
        }
    }
    Py_RETURN_NONE;
}

static PyObject *
qfs_read(PyObject *pself, PyObject *args)
{
    qfs_File *self = (qfs_File *)pself;
    qfs_Client *cl = (qfs_Client *)self->pclient;
    ssize_t rsize = -1l;

    if (!PyArg_ParseTuple(args, "l", &rsize))
        return NULL;

    if (self->fd == -1) {
        SetPyIoError(-EBADF);
        return NULL;
    }

    PyObject *v = PyBytes_FromStringAndSize(NULL, rsize);
    if (v == NULL)
        return NULL;

    char *buf = PyBytes_AsString(v);
    ssize_t nr = cl->client->Read(self->fd, buf, rsize);
    if (nr < 0) {
        Py_DECREF(v);
        SetPyIoError(nr);
        return NULL;
    }
    if (nr != rsize)
        if (PyByteArray_Resize(v, nr) < 0) {
            Py_DECREF(v);
            return NULL;
        }

    if (!self->binary) {
        PyObject *s = PyUnicode_FromEncodedObject(v, self->encoding, self->errors);
        Py_DECREF(v);
        v = s;
    }
    return v;
}

static PyObject *
qfs_write(PyObject *pself, PyObject *args)
{
    qfs_File *self = (qfs_File *)pself;
    qfs_Client *cl = (qfs_Client *)self->pclient;
    Py_ssize_t wsize = -1;
    PyObject *encoded = NULL;
    char *buf = NULL;

    if (!PyArg_ParseTuple(args, self->binary ? "S" : "U", &encoded))
        return NULL;

    if (!self->binary)
        encoded = PyUnicode_AsEncodedString(encoded, self->encoding, self->errors);

    if (!encoded)
        return NULL;

    if (PyBytes_AsStringAndSize(encoded, &buf, &wsize) < 0) {
        if (!self->binary)
            Py_DECREF(encoded);
        return NULL;
    }

    if (wsize < 0) {
        if (!self->binary)
            Py_DECREF(encoded);
        SetPyIoError(-EINVAL);
        return NULL;
    }

    if (self->fd == -1) {
        if (!self->binary)
            Py_DECREF(encoded);
        SetPyIoError(-EBADF);
        return NULL;
    }

    ssize_t nw = cl->client->Write(self->fd, buf, (size_t)wsize);
    if (!self->binary)
        Py_DECREF(encoded);
    if (nw < 0) {
        SetPyIoError(nw);
        return NULL;
    }
    if (nw != wsize) {
        PyObject *msg = PyUnicode_FromFormat(
            "requested write of %d bytes but %ld were written",
            wsize, (long)nw);
        return msg;
    }
    Py_RETURN_NONE;
}

static PyObject *
qfs_chunkLocations(PyObject *pself, PyObject *args)
{
    qfs_File *self = (qfs_File *)pself;
    qfs_Client *cl = (qfs_Client *)self->pclient;
    int off, len;

    if (!PyArg_ParseTuple(args, "i|i", &off, &len))
        return NULL;

    if (self->fd == -1) {
        SetPyIoError(-EBADF);
        return NULL;
    }

    vector<vector <string> > results;

    int s = cl->client->GetDataLocation(self->fd, off, len, results);
    if (s < 0) {
        SetPyIoError(s);
        return NULL;
    }
    size_t n = results.size();
    PyObject *outer = PyTuple_New(n);
    for (size_t i = 0; i < n; i++) {
        size_t nlocs = results[i].size();
        vector<string> locs = results[i];
        PyObject *inner = PyTuple_New(nlocs);
        for (size_t j = 0; j < nlocs; j++) {
            PyTuple_SetItem(inner, j, PyUnicode_FromString(locs[j].c_str()));
        }
        PyTuple_SetItem(outer, i, inner);
    }
    return outer;
}

static PyObject *
qfs_dataVerify(PyObject *pself, PyObject *args)
{
    qfs_File *self = (qfs_File *)pself;
    qfs_Client *cl = (qfs_Client *)self->pclient;

    if (args) {
        PyErr_SetString(PyExc_Exception, "no arguments expected");
        return NULL;
    }

    if (self->fd == -1) {
        SetPyIoError(-EBADF);
        return NULL;
    }

    int res = cl->client->VerifyDataChecksums(self->fd);
    if (res < 0) {
        SetPyIoError(res);
        return NULL;
    }
    return Py_BuildValue("i", res);
}

static PyObject *
qfs_truncate(PyObject *pself, PyObject *args)
{
    qfs_File *self = (qfs_File *)pself;
    qfs_Client *cl = (qfs_Client *)self->pclient;
    off_t off;

    if (!PyArg_ParseTuple(args, "L|i", &off))
        return NULL;

    if (self->fd == -1) {
        SetPyIoError(-EBADF);
        return NULL;
    }

    int s = cl->client->Truncate(self->fd, off);
    if (s < 0) {
        SetPyIoError(s);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
qfs_sync(PyObject *pself, PyObject *args)
{
    qfs_File *self = (qfs_File *)pself;
    qfs_Client *cl = (qfs_Client *)self->pclient;
    int s = cl->client->Sync(self->fd);
    if (s < 0) {
        SetPyIoError(s);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
qfs_seek(PyObject *pself, PyObject *args)
{
    qfs_File *self = (qfs_File *)pself;
    qfs_Client *cl = (qfs_Client *)self->pclient;
    off_t off;
    int whence = SEEK_SET;

    if (!PyArg_ParseTuple(args, "L|i", &off, &whence))
        return NULL;

    if (self->fd == -1) {
        SetPyIoError(-EBADF);
        return NULL;
    }

    off_t s = cl->client->Seek(self->fd, off, whence);
    if (s < 0) {
        SetPyIoError(s);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
qfs_tell(PyObject *pself, PyObject *args)
{
    qfs_File *self = (qfs_File *)pself;
    qfs_Client *cl = (qfs_Client *)self->pclient;

    if (self->fd == -1) {
        SetPyIoError(-EBADF);
        return NULL;
    }

    off_t pos = (off_t)cl->client->Tell(self->fd);
    if (pos < 0) {
        SetPyIoError(pos);
        return NULL;
    }
    return Py_BuildValue("L", pos);
}

static PyMethodDef File_methods[] = {
    { "open",             qfs_reopen,         METH_VARARGS, "Open a closed file." },
    { "close",            qfs_close,          METH_NOARGS,  "Close file." },
    { "read",             qfs_read,           METH_VARARGS, "Read from file." },
    { "write",            qfs_write,          METH_VARARGS, "Write to file." },
    { "truncate",         qfs_truncate,       METH_VARARGS, "Truncate a file." },
    { "chunk_locations",  qfs_chunkLocations, METH_VARARGS, "Get location(s) of a chunk." },
    { "seek",             qfs_seek,           METH_VARARGS, "Seek to file offset." },
    { "tell",             qfs_tell,           METH_NOARGS,  "Return current offset." },
    { "sync",             qfs_sync,           METH_NOARGS,  "Flush file data." },
    { "data_verify",      qfs_dataVerify,     METH_NOARGS,  "Verify data matches what is in QFS."},
    { NULL, NULL }
};

static PyMemberDef File_members[] = {
    { (char*)"name", T_OBJECT, offsetof(qfs_File, name), READONLY, (char*)"file name" },
    { (char*)"mode", T_OBJECT, offsetof(qfs_File, mode), READONLY, (char*)"access mode" },
    { (char*)"fd",   T_INT,    offsetof(qfs_File, fd),   READONLY, (char*)"file descriptor" },
    { NULL }
};

PyDoc_STRVAR(File_doc,
"These objects represent QFS files.  They include a file descriptor (fd)\n"
"that identifies the file to the QFS client library for reads, writes,\n"
"and syncs.  When a file is closed, its fd becomes -1 and no further\n"
"operations can be done on it unless it is reopened with the qfs.file\n"
"open method.\n\n"
"Methods:\n"
"\topen([mode]) -- reopen closed file\n"
"\tclose()     -- close file\n"
"\tread(len)   -- read len bytes, return as string\n"
"\twrite(str)  -- write string to file\n"
"\ttruncate(off) -- truncate file at specified offset\n"
"\tseek(off)   -- seek to specified offset\n"
"\ttell()      -- return current offest\n"
"\tsync()      -- flush file data to server\n"
"\tchunk_locations(path, offset) -- location(s) of the chunk corresponding to offset\n"
"\tdata_verify() -- verify file checksums\n"
"\nData:\n\n"
"\tname        -- the name of the file\n"
"\tmode        -- access mode ('r', 'w', 'r+', or 'w+')\n"
"\tfd          -- file descriptor (-1 if closed)\n");

static PyTypeObject qfs_FileType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "qfs.file",         // tp_name
    sizeof (qfs_File),  // tp_basicsize
    0,                  // tp_itemsize
    File_dealloc,       // tp_dealloc
    0,                  // tp_vectorcall_offset
    0,                  // tp_getattr
    0,                  // tp_setattr
    0,                  // tp_as_async
    File_repr,          // tp_repr
    0,                  // tp_as_number
    0,                  // tp_as_sequence
    0,                  // tp_as_mapping
    0,                  // tp_hash
    0,                  // tp_call
    0,                  // tp_str
    0,                  // tp_getattro
    0,                  // tp_setattro
    0,                  // tp_as_buffer
    Py_TPFLAGS_DEFAULT, // tp_flags
    File_doc,           // tp_doc
    0,                  // tp_traverse
    0,                  // tp_clear
    0,                  // tp_richcompare
    0,                  // tp_weaklistoffest
    0,                  // tp_iter
    0,                  // tp_iternext
    File_methods,       // tp_methods
    File_members,       // tp_members
    0,                  // tp_getset
    0,                  // tp_base
    0,                  // tp_dict
    0,                  // tp_descr_get
    0,                  // tp_descr_set
    0,                  // tp_dictoffset
    File_init,          // tp_init
    0,                  // tp_alloc
    File_new            // tp_new
};

static void
Client_dealloc(PyObject *pself)
{
    qfs_Client *self = (qfs_Client *)pself;
    Py_XDECREF(self->qfshost);
    Py_XDECREF(self->cwd);
    delete self->client;
    Py_TYPE(self)->tp_free(pself);
}

static PyObject *
Client_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    qfs_Client *self = (qfs_Client *)type->tp_alloc(type, 0);

    if (self == NULL)
        return NULL;

    PyObject *host = PyUnicode_FromString("");
    PyObject *cwd  = PyUnicode_FromString("/");
    if (host == NULL || cwd == NULL) {
        Py_DECREF(self);
        return NULL;
    }

    self->qfshost = host;
    self->qfsport = -1;
    self->cwd = cwd;

    return (PyObject *)self;
}

static int
Client_init(PyObject *pself, PyObject *args, PyObject *kwds)
{
    qfs_Client *self = (qfs_Client *)pself;
    char *qfsHost = NULL;
    int qfsPort = -1;

    if (!PyArg_ParseTuple(args, "(si)", &qfsHost, &qfsPort)) {
        return -1;
    }

    KfsClient* client = KFS::Connect(qfsHost, qfsPort);
    if (!client) {
        PyErr_SetString(PyExc_IOError, "Unable to start client.");
        return -1;
    }
    self->client = client;
    PyObject *tmp = self->qfshost;
    self->qfshost = PyUnicode_FromString(qfsHost);
    Py_XDECREF(tmp);
    self->qfsport = qfsPort;

    return 0;
}

static PyObject *
Client_repr(PyObject *pself)
{
    qfs_Client *self = (qfs_Client *)pself;
    return PyUnicode_FromFormat("qfs.client((\'%s\', %d)), cwd=\'%s\'",
                               PyUnicode_AsUTF8(self->qfshost),
                               self->qfsport,
                               PyUnicode_AsUTF8(self->cwd));

}

static string
strip_dots(string path)
{
    vector <string> component;
    string result;
    string::size_type start = 0;

    while (start != string::npos) {
        assert(path[start] == '/');
        string::size_type slash = path.find('/', start + 1);
        string nextc = path.substr(start, slash - start);
        start = slash;
        if (nextc.compare("/..") == 0) {
            if (!component.empty())
                component.pop_back();
        } else if (nextc.compare("/.") != 0)
            component.push_back(nextc);
    }

    if (component.empty())
        component.push_back(string("/"));

    for (vector <string>::iterator c = component.begin();
            c != component.end(); c++) {
        result += *c;
    }
    return result;
}

/*
 * Take a path name that was supplied as an argument for a QFS operation.
 * If it is not absolute, add the current directory to the front of it and
 * in either case, call strip_dots to strip out any "." and ".." components.
 */
static string
build_path(PyObject *cwd, const char *input)
{
    string tail(input);
    if (input[0] == '/')
        return strip_dots(tail);

    const char *c = PyUnicode_AsUTF8(cwd);
    bool is_root = (c[0] == '/' && c[1] == '\0');
    string head(c);
    if (!is_root)
        head.append("/");
    return strip_dots(head + tail);
}

static PyObject *
qfs_cd(PyObject *pself, PyObject *args)
{
    qfs_Client *self = (qfs_Client *)pself;
    char *patharg;

    if (!PyArg_ParseTuple(args, "s", &patharg))
        return NULL;

    string path = build_path(self->cwd, patharg);
    KfsFileAttr attr;
    int status = self->client->Stat(path.c_str(), attr);
    if (status < 0) {
        SetPyIoError(status);
        return NULL;
    }
    if (! attr.isDirectory) {
        SetPyIoError(-ENOTDIR);
        return NULL;
    }
    PyObject *newcwd = PyUnicode_FromString(path.c_str());
    if (newcwd != NULL) {
        Py_DECREF(self->cwd);
        self->cwd = newcwd;
    }
    Py_RETURN_NONE;
}

static PyObject *
qfs_log_level(PyObject *pself, PyObject *args)
{
    qfs_Client *self = (qfs_Client *)pself;
    char *logLevel;

    if (!PyArg_ParseTuple(args, "s", &logLevel))
        return NULL;

    self->client->SetLogLevel(logLevel);
        Py_RETURN_NONE;
}

static PyObject *
qfs_isdir(PyObject *pself, PyObject *args)
{
    qfs_Client *self = (qfs_Client *)pself;
    char *patharg;

    if (!PyArg_ParseTuple(args, "s", &patharg))
        return NULL;

    string path = build_path(self->cwd, patharg);
    bool res = self->client->IsDirectory(path.c_str());
    return Py_BuildValue("b", res);
}

static PyObject *
qfs_isfile(PyObject *pself, PyObject *args)
{
    qfs_Client *self = (qfs_Client *)pself;
    char *patharg;

    if (!PyArg_ParseTuple(args, "s", &patharg))
        return NULL;

    string path = build_path(self->cwd, patharg);
    bool res = self->client->IsFile(path.c_str());
    return Py_BuildValue("b", res);
}

static PyObject *
qfs_mkdir(PyObject *pself, PyObject *args)
{
    qfs_Client *self = (qfs_Client *)pself;
    char *patharg;

    if (!PyArg_ParseTuple(args, "s", &patharg))
        return NULL;

    string path = build_path(self->cwd, patharg);
    int status  = self->client->Mkdir(path.c_str());
    if (status < 0) {
        SetPyIoError(status);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
qfs_mkdirs(PyObject *pself, PyObject *args)
{
    qfs_Client *self = (qfs_Client *)pself;
    char *patharg;

    if (!PyArg_ParseTuple(args, "s", &patharg))
        return NULL;

    string path = build_path(self->cwd, patharg);
    int status  = self->client->Mkdirs(path.c_str());
    if (status < 0) {
        SetPyIoError(status);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
qfs_rmdir(PyObject *pself, PyObject *args)
{
    qfs_Client *self = (qfs_Client *)pself;
    char *patharg;

    if (!PyArg_ParseTuple(args, "s", &patharg))
        return NULL;

    string path = build_path(self->cwd, patharg);
    int status = self->client->Rmdir(path.c_str());
    if (status < 0) {
        SetPyIoError(status);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
qfs_rmdirs(PyObject *pself, PyObject *args)
{
    qfs_Client *self = (qfs_Client *)pself;
    char *patharg;

    if (!PyArg_ParseTuple(args, "s", &patharg))
        return NULL;

    string path = build_path(self->cwd, patharg);
    int status = self->client->Rmdirs(path.c_str());
    if (status < 0) {
        SetPyIoError(status);
        return NULL;
    }
    Py_RETURN_NONE;
}

/*!
 * \brief read directory
 *
 * Return directory contents as a tuple of names
 * XXX It should return a tuple of (name, fid) pairs, but
 * the QFS client readdir code currently only gives names
 */
static PyObject *
qfs_readdir(PyObject *pself, PyObject *args)
{
    qfs_Client *self = (qfs_Client *)pself;
    char *patharg;

    if (!PyArg_ParseTuple(args, "s", &patharg))
        return NULL;

    string path = build_path(self->cwd, patharg);
    vector <string> result;
    int status = self->client->Readdir(path.c_str(), result);
    if (status < 0) {
        SetPyIoError(status);
        return NULL;
    }
    size_t n = result.size();
    PyObject *tuple = PyTuple_New(n);
    for (size_t i = 0; i != n; i++) {
        PyTuple_SetItem(tuple, i,
                PyUnicode_FromString(result[i].c_str()));
    }
    return tuple;
}

/*!
 * \brief Package a KfsFileAttr into a tuple
 */
static PyObject *
package_fattr(KfsFileAttr &fa)
{
    PyObject *tuple = PyTuple_New(7);
    PyTuple_SetItem(tuple, 0, PyUnicode_FromString(fa.filename.c_str()));
    PyTuple_SetItem(tuple, 1, PyLong_FromLongLong(fa.fileId));
    PyTuple_SetItem(tuple, 2, PyUnicode_FromString(ctime(&fa.mtime.tv_sec)));
    PyTuple_SetItem(tuple, 3, PyUnicode_FromString(ctime(&fa.ctime.tv_sec)));
    PyTuple_SetItem(tuple, 4, PyUnicode_FromString(ctime(&fa.crtime.tv_sec)));
    PyTuple_SetItem(tuple, 5, PyUnicode_FromString(
                fa.isDirectory ? "dir" : "file"));
    PyTuple_SetItem(tuple, 6, PyLong_FromLongLong(fa.fileSize));

    return tuple;
}

/*!
 * \brief read directory with attributes
 *
 * Returns a tuple of tuples, each with the following data:
 *
 * (name, fid, mtime, ctime, crtime, type, size)
 */
static PyObject *
qfs_readdirplus(PyObject *pself, PyObject *args)
{
    qfs_Client *self = (qfs_Client *)pself;
    char *patharg;

    if (!PyArg_ParseTuple(args, "s", &patharg))
        return NULL;

    string path = build_path(self->cwd, patharg);

    vector <KfsFileAttr> result;
    int status = self->client->ReaddirPlus(path.c_str(), result);
    if (status < 0) {
        SetPyIoError(status);
        return NULL;
    }
    size_t n = result.size();
    PyObject *outer = PyTuple_New(n);
    for (size_t i = 0; i != n; i++) {
        PyObject *inner = package_fattr(result[i]);
        PyTuple_SetItem(outer, i, inner);
    }
    return outer;
}

static PyObject *
qfs_stat(PyObject *pself, PyObject *args)
{
    qfs_Client *self = (qfs_Client *)pself;
    char *patharg;

    if (!PyArg_ParseTuple(args, "s", &patharg))
        return NULL;

    string path = build_path(self->cwd, patharg);
    KfsFileAttr attr;
    int status = self->client->Stat(path.c_str(), attr, true);
    if (status < 0) {
        SetPyIoError(status);
        return NULL;
    }
    /*
     * Return the stat information in the same format as
     * os.stat() so that we can use the standard stat module
     * on it.
     */
    PyObject *pstat = PyTuple_New(10);
    PyTuple_SetItem(pstat, 0, PyLong_FromLong(
            attr.mode | (attr.isDirectory ? S_IFDIR : 0)));
    PyTuple_SetItem(pstat, 1, PyLong_FromLongLong(attr.fileId));
    PyTuple_SetItem(pstat, 2, PyLong_FromLong(0));  // dev
    PyTuple_SetItem(pstat, 3, PyLong_FromLong(1));   // num links
    PyTuple_SetItem(pstat, 4, PyLong_FromUnsignedLong(attr.user));
    PyTuple_SetItem(pstat, 5, PyLong_FromUnsignedLong(attr.group));
    PyTuple_SetItem(pstat, 6, PyLong_FromLongLong(attr.fileSize));
    PyTuple_SetItem(pstat, 7, PyLong_FromLong(attr.ctime.tv_sec));
    PyTuple_SetItem(pstat, 8, PyLong_FromLong(attr.mtime.tv_sec));
    PyTuple_SetItem(pstat, 9, PyLong_FromLong(attr.crtime.tv_sec));
    return pstat;
}

static PyObject *
qfs_fullstat(PyObject *pself, PyObject *args)
{
    qfs_Client *self = (qfs_Client *)pself;
    char *patharg;

    if (!PyArg_ParseTuple(args, "s", &patharg))
        return NULL;

    string path = build_path(self->cwd, patharg);
    KfsFileAttr attr;
    int status = self->client->Stat(path.c_str(), attr, true);
    if (status < 0) {
        SetPyIoError(status);
        return NULL;
    }
    PyObject *pstat = PyTuple_New(13);
    PyTuple_SetItem(pstat, 0, PyUnicode_FromString(
            attr.isDirectory ? "dir" : "file"));
    PyTuple_SetItem(pstat, 1, PyUnicode_FromString(ctime(&attr.ctime.tv_sec)));
    PyTuple_SetItem(pstat, 2, PyUnicode_FromString(ctime(&attr.mtime.tv_sec)));
    PyTuple_SetItem(pstat, 3, PyLong_FromLongLong(attr.fileSize));
    PyTuple_SetItem(pstat, 4, PyLong_FromLongLong(attr.fileId));
    PyTuple_SetItem(pstat, 5, PyLong_FromLongLong(attr.numReplicas));
    PyTuple_SetItem(pstat, 6, PyLong_FromUnsignedLong(attr.user));
    PyTuple_SetItem(pstat, 7, PyLong_FromUnsignedLong(attr.group));
    PyTuple_SetItem(pstat, 8, PyLong_FromUnsignedLong(attr.mode));
    PyTuple_SetItem(pstat, 9, PyLong_FromLong(
            attr.striperType == KFS::KFS_STRIPED_FILE_TYPE_NONE ? 0 : 1));
    if (attr.striperType != KFS::KFS_STRIPED_FILE_TYPE_NONE) {
        PyTuple_SetItem(pstat, 10, PyLong_FromLong(attr.stripeSize));
        PyTuple_SetItem(pstat, 11, PyLong_FromLong(attr.numStripes));
        PyTuple_SetItem(pstat, 12, PyLong_FromLong(attr.numRecoveryStripes));
    } else {
        PyTuple_SetItem(pstat, 10, PyLong_FromLong(0));
        PyTuple_SetItem(pstat, 11, PyLong_FromLong(0));
        PyTuple_SetItem(pstat, 12, PyLong_FromLong(0));
    }
    return pstat;
}

static PyObject *
qfs_getNumChunks(PyObject *pself, PyObject *args)
{
    qfs_Client *self = (qfs_Client *)pself;
    char *patharg;

    if (!PyArg_ParseTuple(args, "s", &patharg))
        return NULL;

    string path = build_path(self->cwd, patharg);
    int chunkCount = self->client->GetNumChunks(path.c_str());
    if (chunkCount < 0) {
        SetPyIoError(chunkCount);
        return NULL;
    }
    return Py_BuildValue("i", chunkCount);
}

static PyObject *
qfs_getChunkSize(PyObject *pself, PyObject *args)
{
    qfs_Client *self = (qfs_Client *)pself;
    char *patharg;

    if (!PyArg_ParseTuple(args, "s", &patharg))
        return NULL;
    string path = build_path(self->cwd, patharg);
    int chunksz = self->client->GetChunkSize(path.c_str());
    return Py_BuildValue("i", chunksz);
}

static PyObject *
qfs_create(PyObject *pself, PyObject *args)
{
    qfs_Client *self = (qfs_Client *)pself;
    char *patharg;
        int numReplicas = 3;

    if (!PyArg_ParseTuple(args, "s|i", &patharg, &numReplicas))
        return NULL;

    string path = build_path(self->cwd, patharg);
    int fd = self->client->Create(path.c_str(), numReplicas);
    if (fd < 0) {
        SetPyIoError(fd);
        return NULL;
    }

    qfs_File *f = (qfs_File *)qfs_FileType.tp_new(&qfs_FileType, NULL, NULL);
    if (f == NULL ||
            set_file_members(f, path.c_str(), "w", self, fd, NULL, NULL) < 0)
        return NULL;

    return (PyObject *)f;
}

static PyObject *
qfs_remove(PyObject *pself, PyObject *args)
{
    qfs_Client *self = (qfs_Client *)pself;
    char *patharg;

    if (!PyArg_ParseTuple(args, "s", &patharg))
        return NULL;

    string path = build_path(self->cwd, patharg);
    int status = self->client->Remove(path.c_str());
    if (status < 0) {
        SetPyIoError(status);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
qfs_rename(PyObject *pself, PyObject *args)
{
    qfs_Client *self = (qfs_Client *)pself;
    char *srcpath, *dstpath;
    bool overwrite = true;

    if (!PyArg_ParseTuple(args, "ss|b", &srcpath, &dstpath, &overwrite))
        return NULL;

    string spath = build_path(self->cwd, srcpath);
    string dpath = build_path(self->cwd, dstpath);
    int status = self->client->Rename(spath.c_str(), dpath.c_str(), overwrite);
    if (status < 0) {
        SetPyIoError(status);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
qfs_coalesceblocks(PyObject *pself, PyObject *args)
{
    qfs_Client *self = (qfs_Client *)pself;
    char *srcpath, *dstpath;

    if (!PyArg_ParseTuple(args, "ss", &srcpath, &dstpath))
        return NULL;

    string spath = build_path(self->cwd, srcpath);
    string dpath = build_path(self->cwd, dstpath);
        chunkOff_t dstStartOffset;
    int status = self->client->CoalesceBlocks(
            spath.c_str(), dpath.c_str(), &dstStartOffset);
    if (status < 0) {
        SetPyIoError(status);
        return NULL;
    }
    return Py_BuildValue("l", dstStartOffset);
}

static PyObject *
qfs_open(PyObject *pself, PyObject *args)
{
    qfs_Client *self = (qfs_Client *)pself;
    const char *patharg, *modestr = "r", *encoding = NULL, *errors = NULL;

    if (!PyArg_ParseTuple(args, "s|sss", &patharg, &modestr, &encoding, &errors))
        return NULL;

    string path = build_path(self->cwd, patharg);

    qfs_File *f = (qfs_File *)qfs_FileType.tp_new(&qfs_FileType, NULL, NULL);
    if (f == NULL ||
        set_file_members(f, path.c_str(), modestr, self, -1, encoding, errors) < 0) {
        return NULL;
    }
    return (PyObject *)f;
}

static struct PyModuleDef qfs_moduledef = {
    PyModuleDef_HEAD_INIT,
    .m_name = "qfs",
    .m_doc = PyDoc_STR(
"This module links to the QFS client library to provide simple QFS\n"
"file services akin to those for built-in Python file objects.  To use\n"
"it, you must first create a qfs.client object.  This provides the\n"
"connection to QFS; the appropriate QFS servers (i.e., the metaserver\n"
" and chunkservers) must already be active.\n\n"
"Once you have a qfs.client, you can perform file system operations\n"
"corresponding to the QFS client library interfaces and create qfs.file\n"
"objects that represent files in QFS.\n"
    )
};

PyMODINIT_FUNC
PyInit_qfs()
{
    if (PyType_Ready(&qfs_ClientType) < 0 ||
        PyType_Ready(&qfs_FileType) < 0)
        return NULL;

    PyObject *m = PyModule_Create(&qfs_moduledef);

    Py_INCREF(&qfs_ClientType);
    PyModule_AddObject(m, "client", (PyObject *)&qfs_ClientType);
    Py_INCREF(&qfs_FileType);
    PyModule_AddObject(m, "file", (PyObject *)&qfs_FileType);
    return m;
}
