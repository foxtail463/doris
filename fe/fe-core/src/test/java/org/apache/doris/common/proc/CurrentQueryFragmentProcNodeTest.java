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

package org.apache.doris.common.proc;

import org.apache.doris.common.profile.RuntimeProfile;
import org.apache.doris.qe.QueryStatisticsItem;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class CurrentQueryFragmentProcNodeTest {

    @Test
    public void testFetchResultWithFragmentInstanceStatistics() throws Exception {
        TUniqueId instanceId = new TUniqueId(1, 2);
        QueryStatisticsItem item = new QueryStatisticsItem.Builder()
                .queryId("query-1")
                .fragmentInstanceInfos(Lists.newArrayList(
                        buildFragmentInstanceInfo(instanceId, "0", "be1", 9050)))
                .fragmentInstanceStatistics(buildFragmentInstanceStatistics(instanceId, 2048L, 7L))
                .profile(createPipelineOnlyQueryProfile())
                .isReportSucc(true)
                .build();

        List<List<String>> rows = new CurrentQueryFragmentProcNode(item).fetchResult().getRows();
        Assert.assertEquals(1, rows.size());
        Assert.assertEquals("2.00 KB", rows.get(0).get(3));
        Assert.assertEquals("7 Rows", rows.get(0).get(4));
    }

    @Test
    public void testFetchResultWithoutFragmentInstanceStatisticsFallsBackToNA() throws Exception {
        TUniqueId instanceId = new TUniqueId(3, 4);
        QueryStatisticsItem item = new QueryStatisticsItem.Builder()
                .queryId("query-2")
                .fragmentInstanceInfos(Lists.newArrayList(
                        buildFragmentInstanceInfo(instanceId, "0", "be1", 9050)))
                .profile(createPipelineOnlyQueryProfile())
                .isReportSucc(true)
                .build();

        List<List<String>> rows = new CurrentQueryFragmentProcNode(item).fetchResult().getRows();
        Assert.assertEquals(1, rows.size());
        Assert.assertEquals("N/A", rows.get(0).get(3));
        Assert.assertEquals("N/A", rows.get(0).get(4));
    }

    private QueryStatisticsItem.FragmentInstanceInfo buildFragmentInstanceInfo(
            TUniqueId instanceId, String fragmentId, String host, int port) {
        return new QueryStatisticsItem.FragmentInstanceInfo.Builder()
                .instanceId(instanceId)
                .fragmentId(fragmentId)
                .executionAddress(new TNetworkAddress(host, port))
                .build();
    }

    private Map<String, QueryStatisticsItem.FragmentInstanceStatistics> buildFragmentInstanceStatistics(
            TUniqueId instanceId, long scanBytes, long rowsReturned) {
        Map<String, QueryStatisticsItem.FragmentInstanceStatistics> statistics = Maps.newHashMap();
        statistics.put(QueryStatisticsItem.fragmentInstanceStatisticsKey(instanceId),
                new QueryStatisticsItem.FragmentInstanceStatistics.Builder()
                        .instanceId(instanceId)
                        .scanBytes(scanBytes)
                        .rowsReturned(rowsReturned)
                        .done(false)
                        .build());
        return statistics;
    }

    private RuntimeProfile createPipelineOnlyQueryProfile() {
        RuntimeProfile queryProfile = new RuntimeProfile("DetailProfile(query)");
        RuntimeProfile fragmentsProfile = new RuntimeProfile("Fragments");
        RuntimeProfile fragmentProfile = new RuntimeProfile("Fragment 0");
        fragmentProfile.addChild(new RuntimeProfile("FragmentLevelProfile:(host=be1)"), true);
        fragmentProfile.addChild(new RuntimeProfile("Pipeline 0(host=be1)"), true);
        fragmentsProfile.addChild(fragmentProfile, true);
        queryProfile.addChild(fragmentsProfile, true);
        return queryProfile;
    }
}
