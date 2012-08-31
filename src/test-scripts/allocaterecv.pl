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

my $numcmd = shift || 10;

my $start = time();
my $seqf=999000000;
my $seq=$seqf;
my $end=$seq + $numcmd;
my $step=1e4;
my $next=$seq + $step - 1;
if ($next > $end) {
    $next = $end - 1;
}
for (; $seq < $end; $seq++) {
    my $cs = -5;
    my $st = -1;
    $resp = "";
    while (defined($line = <>)) {
        $resp =  $resp . $line;
        if ($cs < 0 && $line =~ /Cseq: (\d+)/) {
            $cs = $1;
        }
        if ($st != 0 && $line =~ /Status: (\d+)/) {
            $st = $1;
        }
        last if ($line eq "\r\n");
    }
    my $err = $cs != $seq || $st != 0;
    if ($seq >= $next || $err) {
        $next += $step;
        if ($next >= $end) {
            $next = $end - 1;
        }
        my $stop = time();
        print $resp;
        my $n = $seq - $seqf + 1;
        printf("elapsed: %d %d %10.3f op/sec\n",
            $stop - $start,
            $n,
            $stop > $start ? ($n) / ($stop - $start) : 0
        );
    }
    last if ($err);
}
printf("done\n");
