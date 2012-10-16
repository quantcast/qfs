/**
 * $Id$
 *
 * Created 2008/07/20
 *
 * @author: Sriram Rao 
 *
 * Copyright 2008-2012 Quantcast Corp.
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
 * 
 * \brief A struct that defines the kfs file attributes that get returned back.
 */

package com.quantcast.qfs.access;

public class KfsFileAttr
{
    public static final int STRIPED_FILE_TYPE_UNKNOWN = 0;
    public static final int STRIPED_FILE_TYPE_NONE    = 1;
    public static final int STRIPED_FILE_TYPE_RS      = 2;
    public static final long KFS_USER_ROOT            = 0;
    public static final long KFS_GROUP_ROOT           = 0;
    public static final long KFS_USER_NONE            = 0xFFFFFFFF;
    public static final long KFS_GROUP_NONE           = 0xFFFFFFFF;
    public static final int  KFS_MODE_UNDEF           = 0xFFFF;

    public KfsFileAttr() {}
    public String  filename;
    public boolean isDirectory;
    public long    filesize;
    public long    modificationTime;
    public int     replication;
    public int     striperType;
    public int     numStripes;
    public int     numRecoveryStripes;
    public int     stripeSize;
    public long    owner;
    public long    group;
    public int     mode;
    public String  ownerName;
    public String  groupName;
    public long    dirCount;
    public long    fileCount;
    public long    fileId;
}
