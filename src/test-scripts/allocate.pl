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

use Socket;
use IO::Handle;

my $numcmd  = shift || 10;
my $remote  = shift || '127.0.0.1';
my $port    = shift || 24000;  # random port
my $fid     = shift || 8; # 68030

socket(SOCK, PF_INET, SOCK_STREAM, getprotobyname('tcp')) or die "socket: $!";
my $iaddr = inet_aton($remote) || die "no host: $remote";
my $paddr = sockaddr_in($port, $iaddr);
connect(SOCK, $paddr) or die "connect: $!";
binmode(SOCK);
SOCK->autoflush(1);

my $start = time();
my $resp = "";
my $seqf=999000000;
my $seq=$seqf;
my $end=$seq + $numcmd;
for (; $seq < $end; $seq++) {
    print SOCK "
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
    my $cs = -5;
    my $st = -1;
    $resp = "";
    while (defined($line = <SOCK>)) {
        $resp =  $resp . $line;
        if ($cs < 0 && $line =~ /Cseq: (\d+)/) {
            $cs = $1;
        }
        if ($st != 0 && $line =~ /Status: (\d+)/) {
            $st = $1;
        }
        last if ($line eq "\r\n");
    }
    last if ($cs != $seq || $st != 0);
}
$stop = time();

close (SOCK) || die "close: $!";

print $resp;
printf("elapsed: %d %d op/sec\n",
    $stop - $start,
    $stop > $start ? ($seq - $seqf) / ($stop - $start) : 0
);
