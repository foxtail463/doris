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

package org.apache.doris.qe;

import org.apache.doris.common.profile.RuntimeProfile;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.thrift.TFragmentInstanceStatistics;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public final class QueryStatisticsItem {

    private final String queryId;
    private final String user;
    private final String sql;
    private final String catalog;
    private final String db;
    private final String connId;
    private final long queryStartTime;
    private final List<FragmentInstanceInfo> fragmentInstanceInfos;
    private final Map<String, FragmentInstanceStatistics> fragmentInstanceStatistics;
    // root query profile
    private final RuntimeProfile queryProfile;
    private final boolean isReportSucc;

    private QueryStatisticsItem(Builder builder) {
        this.queryId = builder.queryId;
        this.user = builder.user;
        this.sql = builder.sql;
        this.catalog = builder.catalog;
        this.db = builder.db;
        this.connId = builder.connId;
        this.queryStartTime = builder.queryStartTime;
        this.fragmentInstanceInfos = builder.fragmentInstanceInfos;
        this.fragmentInstanceStatistics = builder.fragmentInstanceStatistics;
        this.queryProfile = builder.queryProfile;
        this.isReportSucc = builder.isReportSucc;
    }

    public String getDb() {
        return db;
    }

    public String getCatalog() {
        return catalog;
    }

    public String getUser() {
        return user;
    }

    public String getSql() {
        return sql;
    }

    public String getConnId() {
        return connId;
    }

    public String getQueryExecTime() {
        final long currentTime = System.currentTimeMillis();
        if (queryStartTime <= 0) {
            return String.valueOf(-1);
        } else {
            return String.valueOf(currentTime - queryStartTime);
        }
    }

    public String getQueryId() {
        return queryId;
    }

    public List<FragmentInstanceInfo> getFragmentInstanceInfos() {
        return fragmentInstanceInfos;
    }

    public Map<String, FragmentInstanceStatistics> getFragmentInstanceStatistics() {
        return fragmentInstanceStatistics;
    }

    public RuntimeProfile getQueryProfile() {
        return queryProfile;
    }

    public boolean getIsReportSucc() {
        return isReportSucc;
    }

    public static final class Builder {
        private String queryId;
        private String catalog;
        private String db;
        private String user;
        private String sql;
        private String connId;
        private long queryStartTime;
        private List<FragmentInstanceInfo> fragmentInstanceInfos;
        private Map<String, FragmentInstanceStatistics> fragmentInstanceStatistics;
        private RuntimeProfile queryProfile;
        private boolean isReportSucc;

        public Builder() {
            fragmentInstanceInfos = Lists.newArrayList();
            fragmentInstanceStatistics = Maps.newHashMap();
        }

        public Builder queryId(String queryId) {
            this.queryId = queryId;
            return this;
        }

        public Builder db(String db) {
            this.db = db;
            return this;
        }

        public Builder catalog(String catalog) {
            this.catalog = catalog;
            return this;
        }

        public Builder user(String user) {
            this.user = user;
            return this;
        }

        public Builder sql(String sql) {
            this.sql = sql;
            return this;
        }

        public Builder connId(String connId) {
            this.connId = connId;
            return this;
        }

        public Builder queryStartTime(long queryStartTime) {
            this.queryStartTime = queryStartTime;
            return this;
        }

        public Builder fragmentInstanceInfos(List<FragmentInstanceInfo> infos) {
            fragmentInstanceInfos.addAll(infos);
            return this;
        }

        public Builder fragmentInstanceStatistics(Map<String, FragmentInstanceStatistics> statistics) {
            fragmentInstanceStatistics.putAll(statistics);
            return this;
        }

        public Builder profile(RuntimeProfile profile) {
            this.queryProfile = profile;
            return this;
        }

        public Builder isReportSucc(boolean isReportSucc) {
            this.isReportSucc = isReportSucc;
            return this;
        }

        public QueryStatisticsItem build() {
            initDefaultValue(this);
            return new QueryStatisticsItem(this);
        }

        private void initDefaultValue(Builder builder) {
            if (queryId == null) {
                builder.queryId = "0";
            }

            if (db == null) {
                builder.db = "";
            }

            if (sql == null) {
                builder.sql = "";
            }

            if (user == null) {
                builder.user = "";
            }

            if (connId == null) {
                builder.connId = "";
            }

            if (queryProfile == null) {
                queryProfile = new RuntimeProfile("");
            }
        }
    }

    public static final class FragmentInstanceInfo {
        private final TUniqueId instanceId;
        private final TNetworkAddress beHostPort;
        private final TNetworkAddress brpcHostPort;
        private final String fragmentId;

        public FragmentInstanceInfo(Builder builder) {
            this.instanceId = builder.instanceId;
            this.beHostPort = builder.beHostPort;
            this.brpcHostPort = builder.brpcHostPort;
            this.fragmentId = builder.fragmentId;
        }

        public TUniqueId getInstanceId() {
            return instanceId;
        }

        public TNetworkAddress getBeHostPort() {
            return beHostPort;
        }

        public TNetworkAddress getBrpcHostPort() {
            return brpcHostPort;
        }

        public String getFragmentId() {
            return this.fragmentId;
        }

        public static final class Builder {
            private TUniqueId instanceId;
            private TNetworkAddress beHostPort;
            private TNetworkAddress brpcHostPort;
            private String fragmentId;

            public Builder instanceId(TUniqueId instanceId) {
                this.instanceId = instanceId;
                return this;
            }

            public Builder beHostPort(TNetworkAddress beHostPort) {
                this.beHostPort = beHostPort;
                return this;
            }

            public Builder brpcHostPort(TNetworkAddress brpcHostPort) {
                this.brpcHostPort = brpcHostPort;
                return this;
            }

            public Builder fragmentId(String fragmentId) {
                this.fragmentId = fragmentId;
                return this;
            }

            public FragmentInstanceInfo build() {
                initDefaultValue(this);
                return new FragmentInstanceInfo(this);
            }

            private void initDefaultValue(Builder builder) {
                if (builder.instanceId == null) {
                    builder.instanceId = new TUniqueId(-1, -1);
                }

                if (builder.beHostPort == null) {
                    builder.beHostPort = new TNetworkAddress("null", -1);
                }

                if (builder.brpcHostPort == null) {
                    builder.brpcHostPort = builder.beHostPort;
                }

                if (builder.fragmentId == null) {
                    builder.fragmentId = "";
                }
            }
        }
    }

    public static final class FragmentInstanceStatistics {
        private final TUniqueId instanceId;
        private final long scanBytes;
        private final long scanRows;
        private final long rowsReturned;
        private final boolean done;

        public FragmentInstanceStatistics(Builder builder) {
            this.instanceId = builder.instanceId;
            this.scanBytes = builder.scanBytes;
            this.scanRows = builder.scanRows;
            this.rowsReturned = builder.rowsReturned;
            this.done = builder.done;
        }

        public TUniqueId getInstanceId() {
            return instanceId;
        }

        public long getScanBytes() {
            return scanBytes;
        }

        public long getScanRows() {
            return scanRows;
        }

        public long getRowsReturned() {
            return rowsReturned;
        }

        public boolean isDone() {
            return done;
        }

        public static FragmentInstanceStatistics fromThrift(TFragmentInstanceStatistics statistics) {
            return new Builder()
                    .instanceId(statistics.getFragmentInstanceId())
                    .scanBytes(statistics.getScanBytes())
                    .scanRows(statistics.getScanRows())
                    .rowsReturned(statistics.getReturnedRows())
                    .done(statistics.is_done)
                    .build();
        }

        public static final class Builder {
            private TUniqueId instanceId;
            private long scanBytes;
            private long scanRows;
            private long rowsReturned;
            private boolean done;

            public Builder instanceId(TUniqueId instanceId) {
                this.instanceId = instanceId;
                return this;
            }

            public Builder scanBytes(long scanBytes) {
                this.scanBytes = scanBytes;
                return this;
            }

            public Builder scanRows(long scanRows) {
                this.scanRows = scanRows;
                return this;
            }

            public Builder rowsReturned(long rowsReturned) {
                this.rowsReturned = rowsReturned;
                return this;
            }

            public Builder done(boolean done) {
                this.done = done;
                return this;
            }

            public FragmentInstanceStatistics build() {
                if (instanceId == null) {
                    instanceId = new TUniqueId(-1, -1);
                }
                return new FragmentInstanceStatistics(this);
            }
        }
    }

    public static String fragmentInstanceStatisticsKey(TUniqueId instanceId) {
        return DebugUtil.printId(instanceId);
    }
}
