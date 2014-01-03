//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2012/02/23
// Author: Mike Ovsiannikov
//
// Copyright 2012 Quantcast Corp.
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
// \file LayoutManagerInstance.cc
//
//----------------------------------------------------------------------------

#include "LayoutManager.h"

namespace KFS
{
static LayoutManager sLayoutManager;
LayoutManager& gLayoutManager = sLayoutManager;
const UserAndGroup& MetaUserAndGroup::sUserAndGroup = gLayoutManager.GetUserAndGroup();
}
