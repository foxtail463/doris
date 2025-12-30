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

package org.apache.doris.system;

import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.CountingDataOutputStream;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyBackendHostNameOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyBackendOp;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.DataInputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class SystemInfoService {
    private static final Logger LOG = LogManager.getLogger(SystemInfoService.class);

    public static final String DEFAULT_CLUSTER = "default_cluster";

    public static final String NO_BACKEND_LOAD_AVAILABLE_MSG =
            "No backend available for load, please check the status of your backends.";

    public static final String NO_SCAN_NODE_BACKEND_AVAILABLE_MSG =
            "No backend available as scan node, please check the status of your backends.";

    public static final String NOT_USING_VALID_CLUSTER_MSG =
            "Not using valid cloud clusters, please use a cluster before issuing any queries";

    public static final String ERROR_E230 = "E-230";

    public static final ImmutableSet<String> NEED_REPLAN_ERRORS = ImmutableSet.of(
            NO_SCAN_NODE_BACKEND_AVAILABLE_MSG,
            ERROR_E230
    );

    protected volatile ImmutableMap<Long, Backend> idToBackendRef = ImmutableMap.of();
    protected volatile ImmutableMap<Long, AtomicLong> idToReportVersionRef = ImmutableMap.of();

    private volatile ImmutableMap<Long, DiskInfo> pathHashToDiskInfoRef = ImmutableMap.of();

    public static class HostInfo implements Comparable<HostInfo> {
        public String host;
        public int port;

        public HostInfo(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getIdent() {
            return host + "_" + port;
        }

        @Override
        public int compareTo(@NotNull HostInfo o) {
            int res = host.compareTo(o.getHost());
            if (res == 0) {
                return Integer.compare(port, o.getPort());
            }
            return res;
        }

        public boolean isSame(HostInfo other) {
            if (other.getPort() != port) {
                return false;
            }
            return host.equals(other.getHost());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            HostInfo that = (HostInfo) o;
            return Objects.equals(host, that.getHost())
                    && Objects.equals(port, that.getPort());
        }

        @Override
        public String toString() {
            return "HostInfo{"
                    + "host='" + host + '\''
                    + ", port=" + port
                    + '}';
        }
    }

    // sort host backends list by num of backends, descending
    private static final Comparator<List<Backend>> hostBackendsListComparator = new Comparator<List<Backend>>() {
        @Override
        public int compare(List<Backend> list1, List<Backend> list2) {
            if (list1.size() > list2.size()) {
                return -1;
            } else {
                return 1;
            }
        }
    };

    // for deploy manager
    public void addBackends(List<HostInfo> hostInfos, boolean isFree)
            throws UserException {
        addBackends(hostInfos, Tag.DEFAULT_BACKEND_TAG.toMap());
    }

    /**
     * @param hostInfos : backend's ip, hostName and port
     * @throws DdlException
     */
    public void addBackends(List<HostInfo> hostInfos, Map<String, String> tagMap) throws UserException {
        for (HostInfo hostInfo : hostInfos) {
            // check is already exist
            if (getBackendWithHeartbeatPort(hostInfo.getHost(), hostInfo.getPort()) != null) {
                String backendIdentifier = hostInfo.getHost() + ":"
                        + hostInfo.getPort();
                throw new DdlException("Same backend already exists[" + backendIdentifier + "]");
            }
        }

        for (HostInfo hostInfo : hostInfos) {
            addBackend(hostInfo.getHost(), hostInfo.getPort(), tagMap);
        }
    }

    // for test
    public void addBackend(Backend backend) {
        Map<Long, Backend> copiedBackends = Maps.newHashMap(getAllClusterBackendsNoException());
        copiedBackends.put(backend.getId(), backend);
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef = newIdToBackend;
    }

    // Final entry of adding backend
    private void addBackend(String host, int heartbeatPort, Map<String, String> tagMap) {
        Backend newBackend = new Backend(Env.getCurrentEnv().getNextId(), host, heartbeatPort);
        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(getAllClusterBackendsNoException());
        copiedBackends.put(newBackend.getId(), newBackend);
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef = newIdToBackend;

        // set new backend's report version as 0L
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.put(newBackend.getId(), new AtomicLong(0L));
        ImmutableMap<Long, AtomicLong> newIdToReportVersion = ImmutableMap.copyOf(copiedReportVersions);
        idToReportVersionRef = newIdToReportVersion;

        // set tags
        newBackend.setTagMap(tagMap);

        // log
        Env.getCurrentEnv().getEditLog().logAddBackend(newBackend);
        LOG.info("finished to add {} ", newBackend);

        // backends is changed, regenerated tablet number metrics
        MetricRepo.generateBackendsTabletMetrics();
    }

    public void dropBackends(List<HostInfo> hostInfos) throws DdlException {
        for (HostInfo hostInfo : hostInfos) {
            // check is already exist
            if (getBackendWithHeartbeatPort(hostInfo.getHost(), hostInfo.getPort()) == null) {
                String backendIdentifier = NetUtils
                        .getHostPortInAccessibleFormat(hostInfo.getHost(), hostInfo.getPort());
                throw new DdlException("backend does not exists[" + backendIdentifier + "]");
            }
            dropBackend(hostInfo.getHost(), hostInfo.getPort());
        }
    }

    public void dropBackendsByIds(List<String> ids) throws DdlException {

        for (String id : ids) {
            if (getBackend(Long.parseLong(id)) == null) {
                throw new DdlException("backend does not exists[" + id + "]");
            }
            dropBackend(Long.parseLong(id));
        }

    }

    // for decommission
    public void dropBackend(long backendId) throws DdlException {
        Backend backend = getBackend(backendId);
        if (backend == null) {
            throw new DdlException("Backend[" + backendId + "] does not exist");
        }
        dropBackend(backend.getHost(), backend.getHeartbeatPort());
    }

    // final entry of dropping backend
    public void dropBackend(String host, int heartbeatPort) throws DdlException {
        Backend droppedBackend = getBackendWithHeartbeatPort(host, heartbeatPort);
        if (droppedBackend == null) {
            throw new DdlException("backend does not exists[" + NetUtils
                    .getHostPortInAccessibleFormat(host, heartbeatPort) + "]");
        }
        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(getAllClusterBackendsNoException());
        copiedBackends.remove(droppedBackend.getId());
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef = newIdToBackend;

        // update idToReportVersion
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.remove(droppedBackend.getId());
        ImmutableMap<Long, AtomicLong> newIdToReportVersion = ImmutableMap.copyOf(copiedReportVersions);
        idToReportVersionRef = newIdToReportVersion;

        // log
        Env.getCurrentEnv().getEditLog().logDropBackend(droppedBackend);
        LOG.info("finished to drop {}", droppedBackend);

        // backends is changed, regenerated tablet number metrics
        MetricRepo.generateBackendsTabletMetrics();
    }

    public void decommissionBackend(Backend backend) throws UserException {
        // set backend's state as 'decommissioned'
        // for decommission operation, here is no decommission job. the system handler will check
        // all backend in decommission state
        backend.setDecommissioned(true);
        Env.getCurrentEnv().getEditLog().logBackendStateChange(backend);
        LOG.info("set backend {} to decommission", backend.getId());
    }

    // only for test
    public void dropAllBackend() {
        // update idToBackend
        idToBackendRef = ImmutableMap.<Long, Backend>of();
        // update idToReportVersion
        idToReportVersionRef = ImmutableMap.<Long, AtomicLong>of();
    }

    public Backend getBackend(long backendId) {
        return getAllClusterBackendsNoException().get(backendId);
    }

    public List<Backend> getBackends(List<Long> backendIds) {
        List<Backend> backends = Lists.newArrayList();
        for (long backendId : backendIds) {
            Backend backend = getBackend(backendId);
            if (backend != null) {
                backends.add(backend);
            }
        }
        return backends;
    }

    public boolean checkBackendScheduleAvailable(long backendId) {
        Backend backend = getAllClusterBackendsNoException().get(backendId);
        if (backend == null || !backend.isScheduleAvailable()) {
            return false;
        }
        return true;
    }

    public boolean checkBackendAlive(long backendId) {
        Backend backend = getAllClusterBackendsNoException().get(backendId);
        if (backend == null || !backend.isAlive()) {
            return false;
        }
        return true;
    }

    public Backend getBackendWithHeartbeatPort(String host, int heartPort) {
        ImmutableMap<Long, Backend> idToBackend = getAllClusterBackendsNoException();
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(host) && backend.getHeartbeatPort() == heartPort) {
                return backend;
            }
        }
        return null;
    }

    public Backend getBackendWithBePort(String ip, int bePort) {
        ImmutableMap<Long, Backend> idToBackend = getAllClusterBackendsNoException();
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(ip) && backend.getBePort() == bePort) {
                return backend;
            }
        }
        return null;
    }

    public Backend getBackendWithHttpPort(String ip, int httpPort) {
        ImmutableMap<Long, Backend> idToBackend = getAllClusterBackendsNoException();
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(ip) && backend.getHttpPort() == httpPort) {
                return backend;
            }
        }
        return null;
    }

    public List<Long> getAllBackendIds() {
        return getAllBackendIds(false);
    }

    public int getBackendsNumber(boolean needAlive) {
        int beNumber = ConnectContext.get().getSessionVariable().getBeNumberForTest();
        if (beNumber < 0) {
            beNumber = getAllBackendByCurrentCluster(needAlive).size();
        }
        return beNumber;
    }

    public List<Long> getAllBackendByCurrentCluster(boolean needAlive) {
        try {
            ImmutableMap<Long, Backend> bes = getBackendsByCurrentCluster();
            List<Long> beIds = new ArrayList<>(bes.size());
            for (Backend be : bes.values()) {
                if (!needAlive || be.isAlive()) {
                    beIds.add(be.getId());
                }
            }
            return beIds;
        } catch (AnalysisException e) {
            LOG.warn("failed to get backends by Current Cluster", e);
            return Lists.newArrayList();
        }
    }

    public List<Long> getAllBackendIds(boolean needAlive) {
        ImmutableMap<Long, Backend> idToBackend = getAllClusterBackendsNoException();
        List<Long> backendIds = Lists.newArrayList(idToBackend.keySet());
        if (!needAlive) {
            return backendIds;
        } else {
            Iterator<Long> iter = backendIds.iterator();
            while (iter.hasNext()) {
                Backend backend = this.getBackend(iter.next());
                if (backend == null || !backend.isAlive()) {
                    iter.remove();
                }
            }
            return backendIds;
        }
    }

    public List<Backend> getAllClusterBackends(boolean needAlive) {
        return getAllClusterBackendsNoException().values().stream()
                .filter(be -> !needAlive || be.isAlive())
                .collect(Collectors.toList());
    }

    public List<Long> getDecommissionedBackendIds() {
        ImmutableMap<Long, Backend> idToBackend = getAllClusterBackendsNoException();
        List<Long> backendIds = Lists.newArrayList(idToBackend.keySet());

        Iterator<Long> iter = backendIds.iterator();
        while (iter.hasNext()) {
            Backend backend = this.getBackend(iter.next());
            if (backend == null || !backend.isDecommissioned()) {
                iter.remove();
            }
        }
        return backendIds;
    }

    public List<Backend> getMixBackends() {
        return getAllClusterBackendsNoException().values()
                .stream().filter(backend -> backend.isMixNode()).collect(Collectors.toList());
    }

    public List<Backend> getCnBackends() {
        return getAllClusterBackendsNoException()
                .values().stream().filter(Backend::isComputeNode).collect(Collectors.toList());
    }

    // return num of backends that from different hosts
    public int getStorageBackendNumFromDiffHosts(boolean aliveOnly) {
        Set<String> hosts = Sets.newHashSet();
        ImmutableMap<Long, Backend> idToBackend = getAllClusterBackendsNoException();
        for (Backend backend : idToBackend.values()) {
            if ((aliveOnly && !backend.isAlive()) || backend.isComputeNode()) {
                continue;
            }
            hosts.add(backend.getHost());
        }
        return hosts.size();
    }

    class BeIdComparator implements Comparator<Backend> {
        public int compare(Backend a, Backend b) {
            return (int) (a.getId() - b.getId());
        }
    }

    class BeHostComparator implements Comparator<Backend> {
        public int compare(Backend a, Backend b) {
            return a.getHost().compareTo(b.getHost());
        }
    }

    // Select the smallest number of tablets as the starting position of
    // round robin in the BE that match the policy
    public int getStartPosOfRoundRobin(Tag tag, TStorageMedium storageMedium, boolean isStorageMediumSpecified) {
        BeSelectionPolicy.Builder builder = new BeSelectionPolicy.Builder()
                .needScheduleAvailable()
                .needCheckDiskUsage()
                .addTags(Sets.newHashSet(tag))
                .setStorageMedium(storageMedium);
        if (FeConstants.runningUnitTest || Config.allow_replica_on_same_host) {
            builder.allowOnSameHost();
        }

        BeSelectionPolicy policy = builder.build();
        List<Long> beIds = selectBackendIdsByPolicy(policy, -1);
        if (beIds.isEmpty() && storageMedium != null && !isStorageMediumSpecified) {
            storageMedium = (storageMedium == TStorageMedium.HDD) ? TStorageMedium.SSD : TStorageMedium.HDD;
            policy = builder.setStorageMedium(storageMedium).build();
            beIds = selectBackendIdsByPolicy(policy, -1);
        }

        long minBeTabletsNum = Long.MAX_VALUE;
        int minIndex = -1;
        for (int i = 0; i < beIds.size(); ++i) {
            long tabletsNum = Env.getCurrentInvertedIndex().getTabletSizeByBackendId(beIds.get(i));
            if (tabletsNum < minBeTabletsNum) {
                minBeTabletsNum = tabletsNum;
                minIndex = i;
            }
        }
        return minIndex;
    }

    /**
     * 为副本创建选择一组后端节点。
     * 选择后端节点时需要考虑以下参数。
     *
     * @param replicaAlloc 副本分配策略，指定每个标签需要多少个副本
     * @param nextIndexs 创建tablet时的轮询索引，当启用enable_round_robin_create_tablet时使用
     * @param storageMedium 期望的存储介质类型
     * @param isStorageMediumSpecified 存储介质是否被明确指定
     * @param isOnlyForCheck 如果仅用于检查可用后端则设为true
     * @return 返回按标签分组的已选择后端ID，以及实际使用的存储介质
     * @throws DdlException
     */
    public Pair<Map<Tag, List<Long>>, TStorageMedium> selectBackendIdsForReplicaCreation(
            ReplicaAllocation replicaAlloc, Map<Tag, Integer> nextIndexs,
            TStorageMedium storageMedium, boolean isStorageMediumSpecified,
            boolean isOnlyForCheck)
            throws DdlException {
        // 获取集群中所有后端节点的副本，避免并发修改
        Map<Long, Backend> copiedBackends = Maps.newHashMap(getAllClusterBackendsNoException());
        // 存储每个标签选择的后端ID
        Map<Tag, List<Long>> chosenBackendIds = Maps.newHashMap();
        // 获取副本分配映射：标签 -> 副本数量
        Map<Tag, Short> allocMap = replicaAlloc.getAllocMap();
        short totalReplicaNum = 0;

        // 统计存活的后端节点数量
        int aliveBackendNum = (int) copiedBackends.values().stream().filter(Backend::isAlive).count();
        // 检查存活节点数量是否足够创建所需副本
        if (aliveBackendNum < replicaAlloc.getTotalReplicaNum()) {
            throw new DdlException("replication num should be less than the number of available backends. "
                    + "replication num is " + replicaAlloc.getTotalReplicaNum()
                    + ", available backend num is " + aliveBackendNum);
        } else {
            // 记录失败的副本分配信息
            List<String> failedEntries = Lists.newArrayList();

            // 遍历每个标签的副本分配需求
            for (Map.Entry<Tag, Short> entry : allocMap.entrySet()) {
                Tag tag = entry.getKey();
                // 构建后端选择策略
                BeSelectionPolicy.Builder builder = new BeSelectionPolicy.Builder()
                        .needScheduleAvailable()        // 需要可调度的节点
                        .needCheckDiskUsage()          // 需要检查磁盘使用情况
                        .addTags(Sets.newHashSet(entry.getKey()))  // 添加标签约束
                        .setStorageMedium(storageMedium);          // 设置存储介质
                
                // 在测试环境或允许同主机副本时，允许在同一主机上创建副本
                if (FeConstants.runningUnitTest || Config.allow_replica_on_same_host) {
                    builder.allowOnSameHost();
                }
                
                // 如果启用轮询创建tablet，设置轮询索引
                if (Config.enable_round_robin_create_tablet) {
                    builder.setEnableRoundRobin(true);
                    builder.setNextRoundRobinIndex(nextIndexs.getOrDefault(tag, -1));
                }

                // 根据策略选择后端节点
                BeSelectionPolicy policy = builder.build();
                List<Long> beIds = selectBackendIdsByPolicy(policy, entry.getValue());
                
                // 【关键逻辑】第一次选择失败时，尝试使用不同的存储介质
                // 如果仅用于检查，不需要重试不同存储介质
                TStorageMedium originalStorageMedium = storageMedium;
                if (beIds.isEmpty() && storageMedium != null && !isStorageMediumSpecified && !isOnlyForCheck) {
                    // 存储介质切换：HDD <-> SSD
                    storageMedium = (storageMedium == TStorageMedium.HDD) ? TStorageMedium.SSD : TStorageMedium.HDD;
                    builder.setStorageMedium(storageMedium);
                    
                    // 重新设置轮询索引
                    if (Config.enable_round_robin_create_tablet) {
                        builder.setNextRoundRobinIndex(nextIndexs.getOrDefault(tag, -1));
                    }
                    
                    // 使用新存储介质重新选择后端节点
                    policy = builder.build();
                    beIds = selectBackendIdsByPolicy(policy, entry.getValue());
                }
                
                // 更新轮询索引
                if (Config.enable_round_robin_create_tablet) {
                    nextIndexs.put(tag, policy.nextRoundRobinIndex);
                }
                
                // 即使切换存储介质后仍然没有找到合适的后端节点
                if (beIds.isEmpty()) {
                    LOG.error("failed backend(s) for policy: {} real medium {}", policy, originalStorageMedium);
                    String errorReplication = "replication tag: " + entry.getKey()
                            + ", replication num: " + entry.getValue()
                            + ", storage medium: " + originalStorageMedium;
                    failedEntries.add(errorReplication);
                } else {
                    // 成功选择后端节点，记录结果
                    chosenBackendIds.put(entry.getKey(), beIds);
                    totalReplicaNum += beIds.size();
                }
            }

            // 如果有失败的副本分配，抛出异常
            if (!failedEntries.isEmpty()) {
                String failedMsg = Joiner.on("\n").join(failedEntries);
                throw new DdlException("Failed to find enough backend, please check the replication num,"
                        + "replication tag and storage medium and avail capacity of backends "
                        + "or maybe all be on same host." + getDetailsForCreateReplica(replicaAlloc) + "\n"
                        + "Create failed replications:\n" + failedMsg);
            }
        }

        // 验证总副本数量是否正确
        Preconditions.checkState(totalReplicaNum == replicaAlloc.getTotalReplicaNum());
        return Pair.of(chosenBackendIds, storageMedium);
    }

    public String getDetailsForCreateReplica(ReplicaAllocation replicaAlloc) {
        StringBuilder sb = new StringBuilder(" Backends details: ");
        for (Tag tag : replicaAlloc.getAllocMap().keySet()) {
            sb.append("backends with tag ").append(tag).append(" is ");
            sb.append(idToBackendRef.values().stream().filter(be -> be.getLocationTag().equals(tag))
                    .map(Backend::getDetailsForCreateReplica)
                    .collect(Collectors.toList()));
            sb.append(", ");
        }
        return sb.toString();
    }

    public List<Long> selectBackendIdsByPolicy(BeSelectionPolicy policy, int number) {
        return selectBackendIdsByPolicy(policy, number, getAllClusterBackendsNoException().values().asList());
    }

    /**
     * Select a set of backends by the given policy.
     *
     * @param policy if policy is enableRoundRobin, will update its nextRoundRobinIndex
     * @param number number of backends which need to be selected. -1 means return as many as possible.
     * @return return #number of backend ids,
     *         or empty set if no backends match the policy, or the number of matched backends is less than "number";
     */
    public List<Long> selectBackendIdsByPolicy(BeSelectionPolicy policy, int number,
            List<Backend> backendList) {
        Preconditions.checkArgument(number >= -1);

        // 根据策略从输入后端列表中过滤得到候选后端
        List<Backend> candidates = policy.getCandidateBackends(backendList);

        // 候选数不足或为空，直接返回空列表，表示不满足策略要求
        if (candidates.size() < number || candidates.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Not match policy: {}. candidates num: {}, expected: {}", policy, candidates.size(), number);
            }
            return Lists.newArrayList();
        }

        // 若只需要 1 个后端且不启用轮询，随机返回一个即可
        if (number == 1 && !policy.enableRoundRobin) {
            Collections.shuffle(candidates);
            return Lists.newArrayList(candidates.get(0).getId());
        }

        boolean hasSameHost = false;
        if (!policy.allowOnSameHost) {
            // 不允许同主机时：按 host 分组，每个主机随机挑选一个后端，避免同一主机被重复选择
            Map<String, List<Backend>> backendMaps = Maps.newHashMap();
            for (Backend backend : candidates) {
                if (backendMaps.containsKey(backend.getHost())) {
                    backendMaps.get(backend.getHost()).add(backend);
                } else {
                    List<Backend> list = Lists.newArrayList();
                    list.add(backend);
                    backendMaps.put(backend.getHost(), list);
                }
            }

            // 重建候选列表：每个主机最多保留一个后端
            candidates.clear();
            for (List<Backend> list : backendMaps.values()) {
                if (list.size() > 1) {
                    Collections.shuffle(list); // 同一主机多个后端随机化
                    hasSameHost = true;        // 标记存在同主机候选
                }
                candidates.add(list.get(0));
            }
        }

        // 再次校验数量是否足够
        if (candidates.size() < number) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Not match policy: {}. candidates num: {}, expected: {}", policy, candidates.size(), number);
            }
            return Lists.newArrayList();
        }

        if (policy.enableRoundRobin) {
            // 启用轮询：根据是否允许同主机，选择不同的排序维度
            if (!policy.allowOnSameHost && hasSameHost) {
                // 不允许同主机且存在同主机候选时，按 host 排序，保证轮询在主机维度均衡
                Collections.sort(candidates, new BeHostComparator());
            } else {
                // 允许同主机或不存在同主机冲突时，按 BE id 排序，保证轮询稳定顺序
                Collections.sort(candidates, new BeIdComparator());
            }

            // 初始化轮询起点索引（如未设置则随机起点）
            if (policy.nextRoundRobinIndex < 0) {
                policy.nextRoundRobinIndex = new SecureRandom().nextInt(candidates.size());
            }

            // 计算实际起点，按轮询顺序构建偏序列表
            int realIndex = policy.nextRoundRobinIndex % candidates.size();
            List<Long> partialOrderList = new ArrayList<Long>();
            partialOrderList.addAll(candidates.subList(realIndex, candidates.size())
                    .stream().map(Backend::getId).collect(Collectors.toList()));
            partialOrderList.addAll(candidates.subList(0, realIndex)
                    .stream().map(Backend::getId).collect(Collectors.toList()));

            // 截取需要的数量；若为 -1 则返回全部
            List<Long> result = number == -1 ? partialOrderList : partialOrderList.subList(0, number);
            // 推进轮询索引，确保下次选择从下一个位置开始
            policy.nextRoundRobinIndex = realIndex + result.size();

            return result;
        } else {
            // 未启用轮询：打乱候选后按需返回
            Collections.shuffle(candidates);
            if (number != -1) {
                return candidates.subList(0, number).stream().map(Backend::getId).collect(Collectors.toList());
            } else {
                return candidates.stream().map(Backend::getId).collect(Collectors.toList());
            }
        }
    }

    public long getBackendReportVersion(long backendId) {
        AtomicLong atomicLong = null;
        if ((atomicLong = idToReportVersionRef.get(backendId)) == null) {
            return -1L;
        } else {
            return atomicLong.get();
        }
    }

    public void updateBackendReportVersion(long backendId, long newReportVersion, long dbId, long tableId,
            boolean checkDbExist) {
        AtomicLong atomicLong = idToReportVersionRef.get(backendId);
        if (atomicLong == null) {
            return;
        }
        if (checkDbExist && Env.getCurrentInternalCatalog().getDbNullable(dbId) == null) {
            LOG.warn("failed to update backend report version, db {} does not exist", dbId);
            return;
        }
        while (true) {
            long curReportVersion = atomicLong.get();
            if (curReportVersion >= newReportVersion) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("skip update backend {} report version: {}, current version: {}, db: {}, table: {}",
                            backendId, newReportVersion, curReportVersion, dbId, tableId);
                }
                break;
            }
            if (atomicLong.compareAndSet(curReportVersion, newReportVersion)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("update backend {} report version: {}, db: {}, table: {}",
                            backendId, newReportVersion, dbId, tableId);
                }
                break;
            }
        }
    }

    public long saveBackends(CountingDataOutputStream dos, long checksum) throws IOException {
        ImmutableMap<Long, Backend> idToBackend = getAllClusterBackendsNoException();
        int backendCount = idToBackend.size();
        checksum ^= backendCount;
        dos.writeInt(backendCount);
        for (Map.Entry<Long, Backend> entry : idToBackend.entrySet()) {
            long key = entry.getKey();
            checksum ^= key;
            dos.writeLong(key);
            entry.getValue().write(dos);
        }
        return checksum;
    }

    public long loadBackends(DataInputStream dis, long checksum) throws IOException {
        int count = dis.readInt();
        checksum ^= count;
        for (int i = 0; i < count; i++) {
            long key = dis.readLong();
            checksum ^= key;
            Backend backend = Backend.read(dis);
            replayAddBackend(backend);
        }
        return checksum;
    }

    public void clear() {
        this.idToBackendRef = null;
        this.idToReportVersionRef = null;
    }

    public static HostInfo getHostAndPort(String hostPort)
            throws AnalysisException {
        hostPort = hostPort.replaceAll("\\s+", "");
        if (hostPort.isEmpty()) {
            throw new AnalysisException("Invalid host port: " + hostPort);
        }

        HostInfo hostInfo = NetUtils.resolveHostInfoFromHostPort(hostPort);

        String host = hostInfo.getHost();
        if (Strings.isNullOrEmpty(host)) {
            throw new AnalysisException("Host is null");
        }

        int heartbeatPort = -1;
        try {
            // validate port
            heartbeatPort = hostInfo.getPort();
            if (heartbeatPort <= 0 || heartbeatPort >= 65536) {
                throw new AnalysisException("Port is out of range: " + heartbeatPort);
            }

            return new HostInfo(host, heartbeatPort);
        } catch (Exception e) {
            throw new AnalysisException("Encounter unknown exception: " + e.getMessage());
        }
    }


    public static Pair<String, Integer> validateHostAndPort(String hostPort) throws AnalysisException {
        HostInfo hostInfo = getHostAndPort(hostPort);
        return Pair.of(hostInfo.getHost(), hostInfo.getPort());
    }

    public void replayAddBackend(Backend newBackend) {
        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(getAllClusterBackendsNoException());
        copiedBackends.put(newBackend.getId(), newBackend);
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef = newIdToBackend;

        // set new backend's report version as 0L
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.put(newBackend.getId(), new AtomicLong(0L));
        ImmutableMap<Long, AtomicLong> newIdToReportVersion = ImmutableMap.copyOf(copiedReportVersions);
        idToReportVersionRef = newIdToReportVersion;
    }

    public void replayDropBackend(Backend backend) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("replayDropBackend: {}", backend);
        }
        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(getAllClusterBackendsNoException());
        copiedBackends.remove(backend.getId());
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef = newIdToBackend;

        // update idToReportVersion
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.remove(backend.getId());
        ImmutableMap<Long, AtomicLong> newIdToReportVersion = ImmutableMap.copyOf(copiedReportVersions);
        idToReportVersionRef = newIdToReportVersion;

    }

    public void updateBackendState(Backend be) {
        long id = be.getId();
        Backend memoryBe = getBackend(id);
        // backend may already be dropped. this may happen when
        // drop and modify operations do not guarantee the order.
        if (memoryBe != null) {
            memoryBe.setHost(be.getHost());
            memoryBe.setBePort(be.getBePort());
            memoryBe.setAlive(be.isAlive());
            memoryBe.setDecommissioned(be.isDecommissioned());
            memoryBe.setHttpPort(be.getHttpPort());
            memoryBe.setBeRpcPort(be.getBeRpcPort());
            memoryBe.setBrpcPort(be.getBrpcPort());
            memoryBe.setArrowFlightSqlPort(be.getArrowFlightSqlPort());
            memoryBe.setLastUpdateMs(be.getLastUpdateMs());
            memoryBe.setLastStartTime(be.getLastStartTime());
            memoryBe.setDisks(be.getDisks());
            memoryBe.setCpuCores(be.getCputCores());
            memoryBe.setPipelineExecutorSize(be.getPipelineExecutorSize());
        }
    }

    private long getAvailableCapacityB() {
        long capacity = 0L;
        for (Backend backend : getAllClusterBackendsNoException().values()) {
            // Here we do not check if backend is alive,
            // We suppose the dead backends will back to alive later.
            if (backend.isDecommissioned()) {
                // Data on decommissioned backend will move to other backends,
                // So we need to minus size of those data.
                capacity -= backend.getDataUsedCapacityB();
            } else {
                capacity += backend.getAvailableCapacityB();
            }
        }
        return capacity;
    }

    public void checkAvailableCapacity() throws DdlException {
        if (getAvailableCapacityB() <= 0L) {
            throw new DdlException("System has no available disk capacity or no available BE nodes");
        }
    }

    public ImmutableMap<Long, Backend> getAllClusterBackendsNoException() {
        try {
            return getAllBackendsByAllCluster();
        } catch (AnalysisException e) {
            LOG.warn("getAllClusterBackendsNoException: ", e);
            return ImmutableMap.of();
        }
    }

    /*
     * Try to randomly get a backend id by given host.
     * If not found, return -1
     */
    public long getBackendIdByHost(String host) {
        ImmutableMap<Long, Backend> idToBackend = getAllClusterBackendsNoException();
        List<Backend> selectedBackends = Lists.newArrayList();
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(host)) {
                selectedBackends.add(backend);
            }
        }

        if (selectedBackends.isEmpty()) {
            return -1L;
        }

        Collections.shuffle(selectedBackends);
        return selectedBackends.get(0).getId();
    }

    /*
     * Check if the specified disks' capacity has reached the limit.
     * bePathsMap is (BE id -> list of path hash)
     * If floodStage is true, it will check with the floodStage threshold.
     *
     * return Status.OK if not reach the limit
     */
    public Status checkExceedDiskCapacityLimit(Multimap<Long, Long> bePathsMap, boolean floodStage) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("pathBeMap: {}", bePathsMap);
        }
        ImmutableMap<Long, DiskInfo> pathHashToDiskInfo = pathHashToDiskInfoRef;
        for (Long beId : bePathsMap.keySet()) {
            for (Long pathHash : bePathsMap.get(beId)) {
                DiskInfo diskInfo = pathHashToDiskInfo.get(pathHash);
                if (diskInfo != null && diskInfo.exceedLimit(floodStage)) {
                    return new Status(TStatusCode.CANCELLED,
                            "disk " + diskInfo.getRootPath() + " on backend "
                                    + beId + " exceed limit usage, path hash: " + pathHash);
                }
            }
        }
        return Status.OK;
    }

    // update the path info when disk report
    // there is only one thread can update path info, so no need to worry about concurrency control
    public void updatePathInfo(List<DiskInfo> addedDisks, List<DiskInfo> removedDisks) {
        Map<Long, DiskInfo> copiedPathInfos = Maps.newHashMap(pathHashToDiskInfoRef);
        for (DiskInfo diskInfo : addedDisks) {
            copiedPathInfos.put(diskInfo.getPathHash(), diskInfo);
        }
        for (DiskInfo diskInfo : removedDisks) {
            copiedPathInfos.remove(diskInfo.getPathHash());
        }
        ImmutableMap<Long, DiskInfo> newPathInfos = ImmutableMap.copyOf(copiedPathInfos);
        pathHashToDiskInfoRef = newPathInfos;
        if (LOG.isDebugEnabled()) {
            LOG.debug("update path infos: {}", newPathInfos);
        }
    }

    public void modifyBackendHostName(String srcHost, int srcPort, String destHost) throws UserException {
        Backend be = getBackendWithHeartbeatPort(srcHost, srcPort);
        if (be == null) {
            throw new DdlException("backend does not exists[" + NetUtils
                    .getHostPortInAccessibleFormat(srcHost, srcPort) + "]");
        }
        if (be.getHost().equals(destHost)) {
            // no need to modify
            return;
        }
        be.setHost(destHost);
        Env.getCurrentEnv().getEditLog().logModifyBackend(be);
    }

    public void modifyBackendHost(ModifyBackendHostNameOp op) throws UserException {
        Backend be = getBackendWithHeartbeatPort(op.getHost(), op.getPort());
        if (be == null) {
            throw new DdlException("backend does not exists[" + NetUtils
                .getHostPortInAccessibleFormat(op.getHost(), op.getPort()) + "]");
        }
        if (be.getHost().equals(op.getNewHost())) {
            // no need to modify
            return;
        }
        be.setHost(op.getNewHost());
        Env.getCurrentEnv().getEditLog().logModifyBackend(be);
    }

    public void modifyBackends(ModifyBackendOp op) throws UserException {
        List<HostInfo> hostInfos = op.getHostInfos();
        List<Backend> backends = Lists.newArrayList();
        if (hostInfos.isEmpty()) {
            List<String> ids = op.getIds();
            for (String id : ids) {
                long backendId = Long.parseLong(id);
                Backend be = getBackend(backendId);
                if (be == null) {
                    throw new DdlException("backend does not exists[" + backendId + "]");
                }
                backends.add(be);
            }
        } else {
            for (HostInfo hostInfo : hostInfos) {
                Backend be = getBackendWithHeartbeatPort(hostInfo.getHost(), hostInfo.getPort());
                if (be == null) {
                    throw new DdlException(
                          "backend does not exists[" + NetUtils
                                  .getHostPortInAccessibleFormat(hostInfo.getHost(), hostInfo.getPort()) + "]");
                }
                backends.add(be);
            }
        }

        for (Backend be : backends) {
            boolean shouldModify = false;
            Map<String, String> tagMap = op.getTagMap();
            if (!tagMap.isEmpty()) {
                be.setTagMap(tagMap);
                shouldModify = true;
            }

            if (op.isQueryDisabled() != null) {
                shouldModify = be.setQueryDisabled(op.isQueryDisabled());
            }

            if (op.isLoadDisabled() != null) {
                shouldModify = be.setLoadDisabled(op.isLoadDisabled());
            }

            if (shouldModify) {
                Env.getCurrentEnv().getEditLog().logModifyBackend(be);
                LOG.info("finished to modify backend {} ", be);
            }
        }
    }

    public void replayModifyBackend(Backend backend) {
        Backend memBe = getBackend(backend.getId());
        memBe.setTagMap(backend.getTagMap());
        memBe.setQueryDisabled(backend.isQueryDisabled());
        memBe.setLoadDisabled(backend.isLoadDisabled());
        memBe.setHost(backend.getHost());
        if (LOG.isDebugEnabled()) {
            LOG.debug("replay modify backend: {}", backend);
        }
    }

    // Check if there is enough suitable BE for replica allocation
    public void checkReplicaAllocation(ReplicaAllocation replicaAlloc) throws DdlException {
        List<Backend> backends = getMixBackends();
        for (Map.Entry<Tag, Short> entry : replicaAlloc.getAllocMap().entrySet()) {
            if (backends.stream().filter(b -> b.getLocationTag().equals(entry.getKey()))
                    .count() < entry.getValue()) {
                throw new DdlException(
                        "Failed to find enough host with tag(" + entry.getKey() + ") in all backends. need: "
                                + entry.getValue());
            }
        }
    }

    public Set<Tag> getTags() {
        List<Backend> bes = getMixBackends();
        Set<Tag> tags = Sets.newHashSet();
        for (Backend be : bes) {
            tags.add(be.getLocationTag());
        }
        return tags;
    }

    public List<Backend> getBackendsByTag(Tag tag) {
        List<Backend> bes = getMixBackends();
        return bes.stream().filter(b -> b.getLocationTag().equals(tag)).collect(Collectors.toList());
    }

    // CloudSystemInfoService override
    public ImmutableMap<Long, Backend> getBackendsByCurrentCluster() throws AnalysisException {
        return idToBackendRef;
    }

    public ImmutableList<Backend> getBackendListByComputeGroup(Set<String> cgSet) {
        List<Backend> result = new ArrayList<>();
        for (Backend be : idToBackendRef.values()) {
            if (cgSet.contains(be.getLocationTag().value)) {
                result.add(be);
            }
        }
        return ImmutableList.copyOf(result);
    }

    // Cloud and NonCloud get all bes
    public ImmutableMap<Long, Backend> getAllBackendsByAllCluster() throws AnalysisException {
        return idToBackendRef;
    }

    public int getMinPipelineExecutorSize() {
        List<Backend> currentBackends = null;
        try {
            currentBackends = getAllBackendsByAllCluster().values().asList();
        } catch (UserException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("get current cluster backends failed: ", e);
            }
            return 1;
        }
        if (currentBackends.size() == 0) {
            return 1;
        }

        return currentBackends.stream()
                .mapToInt(Backend::getPipelineExecutorSize)
                .filter(size -> size > 0)
                .min()
                .orElse(1);
    }

    // CloudSystemInfoService override
    public int getTabletNumByBackendId(long beId) {
        return Env.getCurrentInvertedIndex().getTabletNumByBackendId(beId);
    }

    // If the error msg contains certain keywords, we need to retry the query with re-plan.
    public static boolean needRetryWithReplan(String errorMsg) {
        if (Strings.isNullOrEmpty(errorMsg)) {
            return false;
        }
        for (String keyword : NEED_REPLAN_ERRORS) {
            if (errorMsg.contains(keyword)) {
                return true;
            }
        }
        return false;
    }
}
