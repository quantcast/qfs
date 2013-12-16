/**
 * $Id$
 *
 * Created 2013/12/15
 *
 * @author: Mike Ovsiannikov 
 *
 * Copyright 2013 Quantcast Corp.
 *
 * This file is part of Quantcast File System
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
 * \brief Class to return create delegation token method result.
 */

package com.quantcast.qfs.access;

public class KfsDelegation
{
    public KfsDelegation() {}
    public boolean delegationAllowedFlag;
    public long    issuedTime;
    public long    tokenValidForSec;
    public long    delegationValidForSec;
    public String  token;
    public String  key;
}
