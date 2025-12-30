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

package org.apache.doris.common.lock;

import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 可监控的可重入读写锁
 * 
 * 这是ReentrantReadWriteLock的监控版本，提供了额外的监控能力：
 * 1. 锁获取监控：记录锁的获取时间
 * 2. 锁持有监控：计算锁被持有的时长
 * 3. 锁释放监控：记录锁的释放时间
 * 4. 死锁检测：检测读写锁的潜在死锁情况
 * 5. 性能分析：帮助诊断锁竞争和性能问题
 * 
 * 主要应用场景：
 * - 数据库元数据保护
 * - 并发访问控制
 * - 性能监控和诊断
 * - 死锁检测和预防
 */
public class MonitoredReentrantReadWriteLock extends ReentrantReadWriteLock {

    private static final Logger LOG = LogManager.getLogger(MonitoredReentrantReadWriteLock.class);
    
    // 监控读锁和写锁实例
    private final ReadLock readLock = new ReadLock(this);
    private final WriteLock writeLock = new WriteLock(this);

    /**
     * 构造方法：创建支持公平性选项的监控锁
     * 
     * @param fair 是否使用公平模式
     *              true: 公平模式，按照请求顺序分配锁
     *              false: 非公平模式，允许锁饥饿但性能更好
     */
    public MonitoredReentrantReadWriteLock(boolean fair) {
        super(fair);
    }

    /**
     * 默认构造方法：创建非公平的监控锁
     * 非公平模式通常性能更好，但可能导致某些线程长时间等待
     */
    public MonitoredReentrantReadWriteLock() {
    }

    /**
     * 监控读锁类
     * 
     * 扩展了ReentrantReadWriteLock.ReadLock，增加了监控能力：
     * - 记录锁获取时间
     * - 计算锁持有时长
     * - 记录锁释放时间
     * 
     * 读锁特点：
     * - 多个线程可以同时持有读锁
     * - 读锁与写锁互斥
     * - 支持重入（同一线程可以多次获取读锁）
     */
    public class ReadLock extends ReentrantReadWriteLock.ReadLock {
        private static final long serialVersionUID = 1L;
        
        // 抽象监控锁实例：用于记录锁操作的时间信息
        private final AbstractMonitoredLock monitor = new AbstractMonitoredLock() {};

        /**
         * 构造方法：创建新的读锁实例
         *
         * @param lock 与此锁关联的ReentrantReadWriteLock实例
         */
        protected ReadLock(ReentrantReadWriteLock lock) {
            super(lock);
        }

        /**
         * 获取读锁
         * 
         * 功能：
         * 1. 调用父类的lock()方法获取读锁
         * 2. 记录锁获取时间，用于后续计算持有时长
         * 
         * 注意事项：
         * - 如果写锁被其他线程持有，此方法会阻塞
         * - 支持重入：同一线程可以多次调用
         */
        @Override
        public void lock() {
            super.lock();        // 获取读锁
            monitor.afterLock(); // 记录锁获取时间
        }

        /**
         * 释放读锁
         * 
         * 功能：
         * 1. 记录锁释放时间
         * 2. 计算并记录锁持有时长
         * 3. 调用父类的unlock()方法释放读锁
         * 
         * 注意事项：
         * - 必须与lock()配对使用
         * - 建议在finally块中调用，确保锁被释放
         */
        @Override
        public void unlock() {
            monitor.afterUnlock(); // 记录锁释放时间并计算持有时长
            super.unlock();        // 释放读锁
        }
    }

    /**
     * 监控写锁类
     * 
     * 扩展了ReentrantReadWriteLock.WriteLock，增加了监控能力：
     * - 记录锁获取时间
     * - 计算锁持有时长
     * - 记录锁释放时间
     * - 死锁检测和警告
     * 
     * 写锁特点：
     * - 独占锁：同一时间只能有一个线程持有写锁
     * - 与读锁互斥：写锁持有期间，其他线程无法获取读锁或写锁
     * - 支持重入：同一线程可以多次获取写锁
     */
    public class WriteLock extends ReentrantReadWriteLock.WriteLock {
        private static final long serialVersionUID = 1L;
        
        // 抽象监控锁实例：用于记录锁操作的时间信息
        private final AbstractMonitoredLock monitor = new AbstractMonitoredLock() {};

        /**
         * 构造方法：创建新的写锁实例
         *
         * @param lock 与此锁关联的ReentrantReadWriteLock实例
         */
        protected WriteLock(ReentrantReadWriteLock lock) {
            super(lock);
        }

        /**
         * 获取写锁
         * 
         * 功能：
         * 1. 调用父类的lock()方法获取写锁
         * 2. 记录锁获取时间，用于后续计算持有时长
         * 3. 死锁检测：在公平模式下检测读写锁的潜在死锁情况
         * 
         * 死锁检测逻辑：
         * - 仅在公平模式下进行检测
         * - 检查当前线程是否同时持有读锁和写锁
         * - 如果检测到潜在死锁，记录警告日志
         * 
         * 注意事项：
         * - 如果读锁或写锁被其他线程持有，此方法会阻塞
         * - 支持重入：同一线程可以多次调用
         * - 死锁检测会产生日志输出，需要合理配置日志级别
         */
        @Override
        public void lock() {
            super.lock();        // 获取写锁
            monitor.afterLock(); // 记录锁获取时间
            
            // 死锁检测：在公平模式下检测读写锁的潜在死锁
            if (isFair() && getReadHoldCount() > 0) {
                // 记录警告日志，包含详细的诊断信息
                LOG.warn(" read lock count is {}, write lock count is {}, stack is {}, query id is {}",
                        getReadHoldCount(),        // 当前线程持有的读锁数量
                        getWriteHoldCount(),       // 当前线程持有的写锁数量
                        Thread.currentThread().getStackTrace(), // 当前线程的堆栈信息
                        ConnectContext.get() == null ? "" : DebugUtil.printId(ConnectContext.get().queryId())); // 查询ID（如果可用）
            }
        }

        /**
         * 释放写锁
         * 
         * 功能：
         * 1. 记录锁释放时间
         * 2. 计算并记录锁持有时长
         * 3. 调用父类的unlock()方法释放写锁
         * 
         * 注意事项：
         * - 必须与lock()配对使用
         * - 建议在finally块中调用，确保锁被释放
         * - 释放后，其他线程可以获取读锁或写锁
         */
        @Override
        public void unlock() {
            monitor.afterUnlock(); // 记录锁释放时间并计算持有时长
            super.unlock();        // 释放写锁
        }
    }

    /**
     * 获取与此锁关联的读锁
     *
     * @return 监控读锁实例
     */
    @Override
    public ReadLock readLock() {
        return readLock;
    }

    /**
     * 获取与此锁关联的写锁
     *
     * @return 监控写锁实例
     */
    @Override
    public WriteLock writeLock() {
        return writeLock;
    }

    /**
     * 获取当前持有写锁的线程
     * 
     * 在公平模式下，返回按照请求顺序获得写锁的线程
     * 在非公平模式下，返回任意获得写锁的线程
     * 
     * @return 持有写锁的线程，如果没有线程持有写锁则返回null
     */
    @Override
    public Thread getOwner() {
        return super.getOwner();
    }
}
