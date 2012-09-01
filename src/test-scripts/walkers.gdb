#
# $Id$
#
# Created 2008
# Author: Mike Ovsiannikov
#
# Copyright 2008-2011 Quantcast Corp.
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
set print asm-demangle on
set print pretty

def no_operation
end

def walk_list
    set $walk_list_nn = $arg0._M_impl._M_node._M_next
    set $walk_list_fn = $walk_list_nn->_M_prev
    while $walk_list_fn != $walk_list_nn
        $arg1 $walk_list_nn
        set $walk_list_nn = $walk_list_nn->_M_next
    end
end

def is_list_empty
    set $is_list_empty_r = \
        $arg0._M_impl._M_node._M_next == &$arg0._M_impl._M_node
end

def list_size_entry
    set $list_size_r = $list_size_r + 1
end

def list_size
    set $list_size_r = 0
    walk_list $arg0 list_size_entry
end

def walk_unordered_map_boost
    set $walk_unordered_map_b = $arg0.m_buckets
    set $walk_unordered_map_l = $walk_unordered_map_b + $arg0.m_bucket_count
    while $walk_unordered_map_b < $walk_unordered_map_l
        set $walk_unordered_map_n = *($walk_unordered_map_b)
        while $walk_unordered_map_n
            $arg1 $walk_unordered_map_n.m_v
            set $walk_unordered_map_n = $walk_unordered_map_n->m_next
        end 
        set $walk_unordered_map_b = $walk_unordered_map_b + 1
    end
end

def walk_unordered_map_std
    set $walk_unordered_map_b = $arg0._M_buckets
    set $walk_unordered_map_l = $walk_unordered_map_b + $arg0._M_bucket_count
    while $walk_unordered_map_b < $walk_unordered_map_l
        set $walk_unordered_map_n = *($walk_unordered_map_b)
        while $walk_unordered_map_n
            $arg1 $walk_unordered_map_n._M_v
            set $walk_unordered_map_n = $walk_unordered_map_n->_M_next
        end 
        set $walk_unordered_map_b = $walk_unordered_map_b + 1
    end
end

def walk_unordered_map
    if $use_walk_unordered_map_boost
        walk_unordered_map_boost $arg0 $arg1
    else
        walk_unordered_map_std $arg0 $arg1
    end
end

def walk_qcdllist
    set $walk_qcdllist_h = $arg0 ? $arg0[$arg1] : 0
    set $walk_qcdllist_n = $walk_qcdllist_h
    while $walk_qcdllist_n
        $arg2 $walk_qcdllist_n
        set $walk_qcdllist_n = $walk_qcdllist_n->mNextPtr[$arg1]
        if $walk_qcdllist_n == $walk_qcdllist_h
            set $walk_qcdllist_n = 0
        end
    end
end

def display_io_buf_entry
    set $display_io_buf_entry_iob = (('std::_List_node<KFS::IOBufferData>' *)$arg0)->_M_data
    set $display_io_buf_list_nb = $display_io_buf_list_nb + \
        ($display_io_buf_entry_iob->mProducer -  $display_io_buf_entry_iob->mConsumer)
    printf "%d %x %d %d %d\n", $display_io_buf_i, \
        ((size_t)$display_io_buf_entry_iob->mConsumer & 4095), \
        $display_io_buf_entry_iob->mProducer -  $display_io_buf_entry_iob->mConsumer, \
        $display_io_buf_list_nb, \
        $display_io_buf_entry_iob->mEnd - $display_io_buf_entry_iob->mProducer
    set $display_io_buf_i = $display_io_buf_i + 1
end

def display_io_buf_list
    set $display_io_buf_i       = 0
    set $display_io_buf_list_nb = 0
    walk_list $arg0 display_io_buf_entry
end

def walk_connection_list
    set $walk_connection_list_slot = 0
    set $walk_connection_list_end = \
        sizeof($arg1->mTimerWheel) / sizeof($arg1->mTimerWheel[0])
    set $walk_connection_list_wheel = $arg1->mTimerWheel
    set $walk_connection_list_i = 0
    while $walk_connection_list_slot < $walk_connection_list_end
        walk_list $walk_connection_list_wheel[$walk_connection_list_slot] $arg0
        set $walk_connection_list_slot = $walk_connection_list_slot + 1
    end
    is_list_empty $arg1->mRemove
    if ! $is_list_empty_r
        printf "connections about to be removed:\n"
        walk_list $arg1->mRemove $arg0
    end
end

def set_connection_entry
    set $connection_entry = \
        (('std::_List_node<boost::shared_ptr<KFS::NetConnection> >' *)$arg0)->_M_data->px
end

def display_connection_entry
    set_connection_entry $arg0
    print *$connection_entry
end

def display_connection_list
    walk_connection_list display_connection_entry  \
        'KFS::libkfsio::Globals_t::sForGdbToFindInstance'.mForGdbToFindNetManager
end

def display_connection_entry_buffers
    set_connection_entry $arg0
    display_connection_buffers_self $connection_entry
end

def display_connection_buffers_self
    set $display_connection_buffers_self_nn = $walk_list_nn
    set $display_connection_buffers_self_fn = $walk_list_fn
    list_size $arg0->mInBuffer.mBuf
    set $display_connection_buffers_self_in_size = $list_size_r
    list_size $arg0->mOutBuffer.mBuf
    set $display_connection_buffers_self_out_size = $list_size_r
    set $walk_list_nn = $display_connection_buffers_self_nn 
    set $walk_list_fn = $display_connection_buffers_self_fn
    printf "%4d %10d %4d %10d %4d %s\n", \
        $total_connection_count, \
        $arg0->mInBuffer.mByteCount, $display_connection_buffers_self_in_size, \
        $arg0->mOutBuffer.mByteCount, $display_connection_buffers_self_out_size, \
        $arg0->mPeerName._M_dataplus._M_p
    set $total_pending_in  = $total_pending_in + $arg0->mInBuffer.mByteCount
    set $total_pending_out = $total_pending_out + $arg0->mOutBuffer.mByteCount
    set $total_in_buf_count  = $total_in_buf_count + $display_connection_buffers_self_in_size
    set $total_out_buf_count = $total_out_buf_count + $display_connection_buffers_self_out_size
    set $total_connection_count = $total_connection_count + 1
end

def display_connections_buffers
    set $total_pending_in       = 0
    set $total_pending_out      = 0
    set $total_in_buf_count     = 0
    set $total_out_buf_count    = 0
    set $total_connection_count = 0
    printf "   #   in bytes bufs  out bytes bufs peer\n"
    walk_connection_list display_connection_entry_buffers \
        'KFS::libkfsio::Globals_t::sForGdbToFindInstance'.mForGdbToFindNetManager
    printf "%4d %10d %4d %10d %4d Total\n", \
        $total_connection_count, \
        $total_pending_in, $total_in_buf_count, \
        $total_pending_out, $total_out_buf_count
end

def display_connections_buffer_manager_entry
    set_connection_entry $arg0
    display_connections_buffer_manager_entry_self \
        (('KFS::ClientSM'*)($connection_entry->mCallbackObj)) \
        $connection_entry->mPeerName._M_dataplus._M_p
end

def display_connections_buffer_manager_entry_self
    if $arg0->mManagerPtr ==  &sDiskIoQueuesPtr->mBufferManager
        printf "%4d %10d %10d %s\n", \
            $total_connection_buffer_manager_count, \
            $arg0->mByteCount, \
            $arg0->mWaitingForByteCount, \
            $arg1
        # ($arg0->mCurOp ? (('KFS::RecordAppendOp'*)$arg0->mCurOp)->numBytes : 0)
    end
    set $total_connection_buffer_manager_count = $total_connection_buffer_manager_count + 1
end

def display_connections_buffer_manager
    set $total_connection_buffer_manager_count = 0
    walk_connection_list display_connections_buffer_manager_entry \
        'KFS::libkfsio::Globals_t::sForGdbToFindInstance'.mForGdbToFindNetManager
end

def display_atomic_appender_entry
    p $arg0->second.px
end

def display_atomic_appenders
    walk_unordered_map \
        'KFS::gAtomicRecordAppendManager'.mAppenders \
        display_atomic_appender_entry
end

def display_atomic_appender_entry_buffers
    set $ara_buf = $arg0->second.px.mBuffer
    list_size $ara_buf.mBuf
    printf "%4d %10d %4d %ld\n", \
        $ara_count, \
        $ara_buf.mByteCount, \
        $list_size_r, \
        $arg0->second.px.mChunkId
    set $ara_count = $ara_count + 1
    set $total_ara_byte_count = $total_ara_byte_count + $ara_buf.mByteCount
    set $total_ara_buf_count  = $total_ara_buf_count + $list_size_r
end

def display_atomic_appenders_buffers
    set $ara_count = 0
    set $total_ara_byte_count = 0
    set $total_ara_buf_count  = 0
    printf "   #      bytes bufs chunk\n"
    walk_unordered_map \
        'KFS::gAtomicRecordAppendManager'.mAppenders \
        display_atomic_appender_entry_buffers
    printf "%4d %10d %4d Total\n", \
        $ara_count, \
        $total_ara_byte_count, \
        $total_ara_buf_count
end

def display_disk_queue_entry_buffers
    printf "%2d %4d %10ld %10ld %s\n", \
        $total_disk_queue_count, \
        $arg0->mQueuePtr->mPendingCount, \
        $arg0->mQueuePtr->mPendingReadBlockCount, \
        $arg0->mQueuePtr->mPendingWriteBlockCount, \
        $arg0->mFileNamePrefixes._M_dataplus._M_p
    set $total_disk_queue_count = $total_disk_queue_count + 1
    set $total_request_count    = $total_request_count + $arg0->mQueuePtr->mPendingCount
    set $total_read_blk_count   = $total_read_blk_count + $arg0->mQueuePtr->mPendingReadBlockCount
    set $total_write_blk_count  = $total_write_blk_count + $arg0->mQueuePtr->mPendingWriteBlockCount
end

def display_disk_queues_buffers
    set $total_disk_queue_count = 0
    set $total_request_count    = 0
    set $total_read_blk_count   = 0
    set $total_write_blk_count  = 0
    printf " # req.  read bufs write bufs prefix\n"
    walk_qcdllist sDiskIoQueuesPtr->mDiskQueuesPtr 0 display_disk_queue_entry_buffers
    printf "%2d %4d %10ld %10ld Total\n", \
        $total_disk_queue_count, \
        $total_request_count, \
        $total_read_blk_count, \
        $total_write_blk_count
    printf "read  total: %14ld bytes %5d req.\nwrite total: %14ld bytes %5d req.\n", \
        sDiskIoQueuesPtr->mReadPendingBytes, \
        sDiskIoQueuesPtr->mReadReqCount, \
        sDiskIoQueuesPtr->mWritePendingBytes, \
        sDiskIoQueuesPtr->mWriteReqCount
end

def buffer_pool_partition
    printf "%2d %8d %8d\n", \
        $total_pool_partitions, $arg0->mTotalCnt, $arg0->mFreeCnt
    set $total_pool_partitions     = $total_pool_partitions + 1
    set $total_pool_buf_count      = $total_pool_buf_count + $arg0->mTotalCnt
    set $total_pool_free_buf_count = $total_pool_free_buf_count + $arg0->mFreeCnt
end

def display_buffer_pool
    set $total_pool_partitions     = 0
    set $total_pool_buf_count      = 0
    set $total_pool_free_buf_count = 0
    printf " #    total     free\n"
    walk_qcdllist sDiskIoQueuesPtr->mBufferAllocator.mBufferPool.mPartitionListPtr \
        0 buffer_pool_partition
    printf "%2d %8d %8d Total\n", \
        $total_pool_partitions, $total_pool_buf_count, $total_pool_free_buf_count
end

def display_io_buffers
    printf "----- connections:\n"
    display_connections_buffers
    printf "----- record appenders:\n"
    display_atomic_appenders_buffers
    printf "----- disk queues:\n"
    display_disk_queues_buffers
    # Don't count disk pending read -- 
    # the buffers are allocated right before the read request starts.
    # This doesn't take into the accout the read buffers in io completion
    # queue
    printf "----- \ngrand total: %ld bytes %ld buffers\n", \
        $total_pending_in + $total_pending_out + \
        $total_ara_byte_count + sDiskIoQueuesPtr->mWritePendingBytes, \
        $total_in_buf_count + $total_out_buf_count + \
        $total_ara_buf_count + \
        $total_write_blk_count
    printf "----- buffer pool:\n"
    display_buffer_pool
    printf "------\n%d buffers unaccounted for\n", \
        $total_pool_buf_count - $total_pool_free_buf_count - (\
        $total_in_buf_count + $total_out_buf_count + \
        $total_ara_buf_count + \
        $total_write_blk_count)
end

def display_timeout_entry
    output $arg0->mNextPtr[0]->mPrevPtr[0]
    printf "\n"
end

def display_timeout_list
    walk_qcdllist $arg0 0 display_timeout_entry
end

def display_net_manager_timeout_list
    display_timeout_list 'KFS::libkfsio::Globals_t::sForGdbToFindInstance'.mForGdbToFindNetManager.mTimeoutHandlers
end

def display_kfanout_pending_input
    printf "end of intput: %d\n", (int)sDebugCoreInfo.mFanOutPtr->mEndOfInputFlag
    printf "maxReadAhead: %d\n", \
        sDebugCoreInfo.mFanOutPtr->mConnPtr.px->maxReadAhead
    p sDebugCoreInfo.mFanOutPtr->mConnPtr.px->mNetManagerEntry
    printf "readAhead: %d\n", \
        sDebugCoreInfo.mFanOutPtr->mConnPtr.px->mInBuffer.mByteCount
    set $d=(('std::_List_node<KFS::IOBufferData>'*)sDebugCoreInfo.mFanOutPtr->mConnPtr.px->mInBuffer.mBuf._M_impl._M_node._M_next)->_M_data          
    printf "size: %d\n", (int)($d.mProducer - $d.mConsumer)
    set $v=$d.mConsumer
    printf "rec len: %d\n", (int)(($v[0]<<24)+($v[1]<<16)+($v[2]<<8)+($v[3]))
end
