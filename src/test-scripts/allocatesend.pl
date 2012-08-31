eval 'exec perl -wS $0 ${1+"$@"}'
  if 0;
#
# $Id$
#
# Created 2010
# Author: Mike Ovsiannikov
#
# Copyright 2010 Quantcast Corp.
#
# This file is part of Kosmos File System (KFS).
#
# Licensed under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#
#

my $numcmd  = shift || 10;
my $fid     = shift || 8; # 68030

my $seqf=999000000;
my $seq=$seqf;
my $end=$seq + $numcmd;
for (; $seq < $end; $seq++) {
    print "
ALLOCATE\r
Cseq: $seq\r
Version: KFS/1.0\r
Client-Protocol-Version: 100\r
Client-host: somehostname\r
Pathname: /sort/job/1/fanout/27/file.27\r
File-handle: $fid\r
Chunk-offset: 0\r
Chunk-append: 1\r
Space-reserve: 0\r
Max-appenders: 640000000\r
\r
";
}
printf("==== test done ===\r\n\r\n");
