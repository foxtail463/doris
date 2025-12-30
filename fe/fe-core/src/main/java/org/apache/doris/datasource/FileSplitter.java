// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.datasource;

import org.apache.doris.common.util.LocationPath;
import org.apache.doris.common.util.Util;
import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TFileCompressType;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;

public class FileSplitter {
    private static final Logger LOG = LogManager.getLogger(FileSplitter.class);

    // If the number of files is larger than parallel instances * num of backends,
    // we don't need to split the file.
    // Otherwise, split the file to avoid local shuffle.
    public static boolean needSplitForCountPushdown(int parallelism, int numBackends, long totalFileNum) {
        return totalFileNum < parallelism * numBackends;
    }

    /**
     * 将文件分割成多个 Split，用于并行处理
     * 
     * 如果文件不可分割（如压缩文件）或标记为不可分割，则返回包含整个文件的单个 Split。
     * 如果文件可分割，则按照 fileSplitSize 将文件分割成多个 Split，每个 Split 包含文件的一部分数据。
     * 
     * @param path 文件路径
     * @param fileSplitSize 每个 Split 的目标大小（字节）
     * @param blockLocations 文件的块位置信息数组，用于确定数据所在的节点
     * @param length 文件总长度（字节）
     * @param modificationTime 文件修改时间
     * @param splittable 标识文件是否可分割
     * @param partitionValues 分区值列表，用于标识该文件所属的分区
     * @param splitCreator Split 创建器，用于创建具体的 Split 对象
     * @return 分割后的 Split 列表
     * @throws IOException 如果发生 IO 错误
     */
    public static List<Split> splitFile(
            LocationPath path,
            long fileSplitSize,
            BlockLocation[] blockLocations,
            long length,
            long modificationTime,
            boolean splittable,
            List<String> partitionValues,
            SplitCreator splitCreator)
            throws IOException {
        // 如果块位置信息为 null，初始化为空数组
        if (blockLocations == null) {
            blockLocations = new BlockLocation[0];
        }
        List<Split> result = Lists.newArrayList();
        // 根据文件路径推断压缩类型
        TFileCompressType compressType = Util.inferFileCompressTypeByPath(path.getNormalizedLocation());
        // 如果文件不可分割或者是压缩文件（非 PLAIN 类型），则创建包含整个文件的单个 Split
        if (!splittable || compressType != TFileCompressType.PLAIN) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Path {} is not splittable.", path);
            }
            // 获取第一个块的主机信息，用于数据本地性优化
            String[] hosts = blockLocations.length == 0 ? null : blockLocations[0].getHosts();
            // 创建包含整个文件的 Split（起始位置 0，长度 length）
            result.add(splitCreator.create(path, 0, length, length, fileSplitSize,
                    modificationTime, hosts, partitionValues));
            return result;
        }
        // 文件可分割，按照 fileSplitSize 进行分割
        long bytesRemaining;
        // 循环创建 Split，直到剩余字节数小于等于 1.1 * fileSplitSize
        // 使用 1.1 的阈值是为了避免创建过小的最后一个 Split
        for (bytesRemaining = length; (double) bytesRemaining / (double) fileSplitSize > 1.1D;
                bytesRemaining -= fileSplitSize) {
            // 根据当前偏移量找到对应的块位置索引
            int location = getBlockIndex(blockLocations, length - bytesRemaining);
            // 获取该块所在的主机信息
            String[] hosts = location == -1 ? null : blockLocations[location].getHosts();
            // 创建一个 Split：起始位置为 length - bytesRemaining，大小为 fileSplitSize
            result.add(splitCreator.create(path, length - bytesRemaining, fileSplitSize,
                    length, fileSplitSize, modificationTime, hosts, partitionValues));
        }
        // 处理剩余的字节（最后一个 Split）
        if (bytesRemaining != 0L) {
            int location = getBlockIndex(blockLocations, length - bytesRemaining);
            String[] hosts = location == -1 ? null : blockLocations[location].getHosts();
            // 创建最后一个 Split：大小为剩余的 bytesRemaining 字节
            result.add(splitCreator.create(path, length - bytesRemaining, bytesRemaining,
                    length, fileSplitSize, modificationTime, hosts, partitionValues));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Path {} includes {} splits.", path, result.size());
        }
        return result;
    }

    private static int getBlockIndex(BlockLocation[] blkLocations, long offset) {
        if (blkLocations == null || blkLocations.length == 0) {
            return -1;
        }
        for (int i = 0; i < blkLocations.length; ++i) {
            if (blkLocations[i].getOffset() <= offset
                    && offset < blkLocations[i].getOffset() + blkLocations[i].getLength()) {
                return i;
            }
        }
        BlockLocation last = blkLocations[blkLocations.length - 1];
        long fileLength = last.getOffset() + last.getLength() - 1L;
        throw new IllegalArgumentException(String.format("Offset %d is outside of file (0..%d)", offset, fileLength));
    }


}
