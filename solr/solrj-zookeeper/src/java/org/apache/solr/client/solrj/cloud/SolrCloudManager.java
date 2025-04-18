/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.client.solrj.cloud;

import java.io.IOException;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.util.ObjectCache;
import org.apache.solr.common.util.TimeSource;

/**
 * This interface abstracts the access to a SolrCloud cluster, including interactions with
 * Zookeeper, Solr.
 *
 * <p>This abstraction should be used when possible instead of directly referencing ZK, Solr and
 * HTTP.
 */
public interface SolrCloudManager extends SolrCloseable {

  CloudSolrClient getSolrClient();

  ClusterStateProvider getClusterStateProvider();

  default ClusterState getClusterState() throws IOException {
    return getClusterStateProvider().getClusterState();
  }

  NodeStateProvider getNodeStateProvider();

  DistribStateManager getDistribStateManager();

  ObjectCache getObjectCache();

  TimeSource getTimeSource();

  @Deprecated
  <T extends SolrResponse> T request(SolrRequest<T> req) throws IOException;
}
