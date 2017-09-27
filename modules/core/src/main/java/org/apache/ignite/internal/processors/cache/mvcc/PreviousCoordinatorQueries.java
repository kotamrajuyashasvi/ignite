/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
class PreviousCoordinatorQueries {
    /** */
    private volatile boolean prevQueriesDone;

    /** */
    private final ConcurrentHashMap<UUID, Map<MvccCounter, Integer>> activeQueries = new ConcurrentHashMap<>();

    /** */
    private Set<UUID> rcvd;

    /** */
    private Set<UUID> waitNodes;

    /** */
    private boolean initDone;

    void init(Map<UUID, Map<MvccCounter, Integer>> srvNodesQueries, DiscoCache discoCache, GridDiscoveryManager mgr) {
        synchronized (this) {
            assert !initDone;
            assert waitNodes == null;

            waitNodes = new HashSet<>();

            for (ClusterNode node : discoCache.allNodes()) {
                if (CU.clientNode(node) && mgr.alive(node) && !F.contains(rcvd, node.id()))
                    waitNodes.add(node.id());
            }

            initDone = waitNodes.isEmpty();

            if (srvNodesQueries != null) {
                for (Map.Entry<UUID, Map<MvccCounter, Integer>> e : srvNodesQueries.entrySet())
                    addAwaitedActiveQueries(e.getKey(), e.getValue());
            }

            if (initDone)
                prevQueriesDone = activeQueries.isEmpty();
        }
    }

    boolean previousQueriesDone() {
        return prevQueriesDone;
    }

    private void addAwaitedActiveQueries(UUID nodeId, Map<MvccCounter, Integer> nodeQueries) {
        if (nodeQueries == null || prevQueriesDone)
            return;

        Map<MvccCounter, Integer> queries = activeQueries.get(nodeId);

        if (queries == null)
            activeQueries.put(nodeId, nodeQueries);
        else {
            for (Map.Entry<MvccCounter, Integer> e : nodeQueries.entrySet()) {
                Integer qryCnt = queries.get(e.getKey());

                int newQryCnt = (qryCnt == null ? 0 : qryCnt) + e.getValue();

                if (newQryCnt == 0) {
                    queries.remove(e.getKey());

                    if (queries.isEmpty())
                        activeQueries.remove(nodeId);
                }
                else
                    queries.put(e.getKey(), newQryCnt);
            }
        }

        prevQueriesDone = activeQueries.isEmpty();
    }

    void processClientActiveQueries(UUID nodeId, @Nullable Map<MvccCounter, Integer> activeQueries) {
        synchronized (this) {
            if (initDone)
                return;

            if (waitNodes == null) {
                if (rcvd == null)
                    rcvd = new HashSet<>();

                rcvd.add(nodeId);
            }
            else
                initDone = waitNodes.remove(nodeId);

            addAwaitedActiveQueries(nodeId, activeQueries);
        }
    }

    /**
     * @param nodeId Failed node ID.
     */
    void onNodeLeft(UUID nodeId) {
        synchronized (this) {
            initDone = waitNodes != null && waitNodes.remove(nodeId);

            if (initDone && !prevQueriesDone && activeQueries.remove(nodeId) != null)
                prevQueriesDone = activeQueries.isEmpty();
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     */
    void onQueryDone(UUID nodeId, NewCoordinatorQueryAckRequest msg) {
        synchronized (this) {
            MvccCounter cntr = new MvccCounter(msg.coordinatorVersion(), msg.counter());

            Map<MvccCounter, Integer> nodeQueries = activeQueries.get(nodeId);

            if (nodeQueries == null)
                activeQueries.put(nodeId, nodeQueries = new HashMap<>());

            Integer qryCnt = nodeQueries.get(cntr);

            int newQryCnt = (qryCnt != null ? qryCnt : 0) - 1;

            if (initDone) {
                if (newQryCnt == 0) {
                    nodeQueries.remove(cntr);

                    if (nodeQueries.isEmpty())
                        activeQueries.remove(nodeId);

                    prevQueriesDone = activeQueries.isEmpty();
                }
            }
            else
                nodeQueries.put(cntr, newQryCnt);
        }
    }
}
