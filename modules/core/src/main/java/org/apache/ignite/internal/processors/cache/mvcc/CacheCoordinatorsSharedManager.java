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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;
import org.jsr166.LongAdder8;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE_COORDINATOR;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

/**
 *
 */
public class CacheCoordinatorsSharedManager<K, V> extends GridCacheSharedManagerAdapter<K, V> {
    /** */
    public static final long COUNTER_NA = 0L;

    /** */
    private static final boolean STAT_CNTRS = false;

    /** */
    private static final GridTopic MSG_TOPIC = TOPIC_CACHE_COORDINATOR;

    /** */
    private static final byte MSG_POLICY = SYSTEM_POOL;
    
    /** */
    private volatile MvccCoordinator curCrd;

    /** */
    private final AtomicLong mvccCntr = new AtomicLong(1L);

    /** */
    private final GridAtomicLong committedCntr = new GridAtomicLong(1L);

    /** */
    private final ConcurrentSkipListMap<Long, GridCacheVersion> activeTxs = new ConcurrentSkipListMap<>();

    /** */
    private final ConcurrentMap<Long, AtomicInteger> activeQueries = new ConcurrentHashMap<>();

    /** */
    private final ConcurrentMap<Long, MvccVersionFuture> verFuts = new ConcurrentHashMap<>();

    /** */
    private final ConcurrentMap<Long, WaitAckFuture> ackFuts = new ConcurrentHashMap<>();

    /** */
    private ConcurrentMap<Long, WaitTxFuture> waitTxFuts = new ConcurrentHashMap<>();

    /** */
    private final AtomicLong futIdCntr = new AtomicLong();

    /** */
    private final CountDownLatch crdLatch = new CountDownLatch(1);

    /** Topology version when local node was assigned as coordinator. */
    private long crdVer;

    /** */
    private StatCounter[] statCntrs;

    /**
     * @param ver1 First version.
     * @param ver2 Second version.
     * @return
     */
    public static int compareVersions(MvccCoordinatorVersion ver1, MvccCoordinatorVersion ver2) {
        assert ver1 != null;
        assert ver2 != null;

        int cmp = Long.compare(ver1.coordinatorVersion(), ver2.coordinatorVersion());

        if (cmp != 0)
            return cmp;

        return Long.compare(ver1.counter(), ver2.counter());
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        statCntrs = new StatCounter[7];

        statCntrs[0] = new CounterWithAvg("CoordinatorTxCounterRequest", "avgTxs");
        statCntrs[1] = new CounterWithAvg("MvccCoordinatorVersionResponse", "avgFutTime");
        statCntrs[2] = new StatCounter("CoordinatorTxAckRequest");
        statCntrs[3] = new CounterWithAvg("CoordinatorTxAckResponse", "avgFutTime");
        statCntrs[4] = new StatCounter("TotalRequests");
        statCntrs[5] = new StatCounter("CoordinatorWaitTxsRequest");
        statCntrs[6] = new CounterWithAvg("CoordinatorWaitTxsResponse", "avgFutTime");

        cctx.gridEvents().addLocalEventListener(new CacheCoordinatorDiscoveryListener(),
            EVT_NODE_FAILED, EVT_NODE_LEFT);

        cctx.gridIO().addMessageListener(MSG_TOPIC, new CoordinatorMessageListener());
    }

    /**
     * @param log Logger.
     */
    public void dumpStatistics(IgniteLogger log) {
        if (STAT_CNTRS) {
            log.info("Mvcc coordinator statistics: ");

            for (StatCounter cntr : statCntrs)
                cntr.dumpInfo(log);
        }
    }

    /**
     * @param tx Transaction.
     * @return Counter.
     */
    public MvccCoordinatorVersion requestTxCounterOnCoordinator(IgniteInternalTx tx) {
        assert cctx.localNode().equals(currentCoordinatorNode());

        return assignTxCounter(tx.nearXidVersion(), 0L);
    }

    /**
     * @param crd Coordinator.
     * @param lsnr Response listener.
     * @return Counter request future.
     */
    public IgniteInternalFuture<MvccCoordinatorVersion> requestTxCounter(ClusterNode crd, MvccResponseListener lsnr, GridCacheVersion txVer) {
        assert !crd.isLocal() : crd;

        MvccVersionFuture fut = new MvccVersionFuture(futIdCntr.incrementAndGet(),
            crd,
            lsnr);

        verFuts.put(fut.id, fut);

        try {
            cctx.gridIO().sendToGridTopic(crd,
                MSG_TOPIC,
                new CoordinatorTxCounterRequest(fut.id, txVer),
                MSG_POLICY);
        }
        catch (IgniteCheckedException e) {
            fut.onError(e);
        }

        return fut;
    }

    /**
     * @param crd Coordinator.
     * @param mvccVer Query version.
     */
    public void ackQueryDone(ClusterNode crd, MvccCoordinatorVersion mvccVer) {
        try {
            long trackCntr = mvccVer.counter();

            MvccLongList txs = mvccVer.activeTransactions();

            if (txs != null) {
                for (int i = 0; i < txs.size(); i++) {
                    long txId = txs.get(i);

                    if (txId < trackCntr)
                        trackCntr = txId;
                }
            }

            cctx.gridIO().sendToGridTopic(crd,
                MSG_TOPIC,
                new CoordinatorQueryAckRequest(trackCntr),
                MSG_POLICY);
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send query ack, node left [crd=" + crd + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send query ack [crd=" + crd + ", cntr=" + mvccVer + ']', e);
        }
    }

    /**
     * @param crd Coordinator.
     * @return Counter request future.
     */
    public IgniteInternalFuture<MvccCoordinatorVersion> requestQueryCounter(ClusterNode crd) {
        assert crd != null;

        // TODO IGNITE-3478: special case for local?
        MvccVersionFuture fut = new MvccVersionFuture(futIdCntr.incrementAndGet(), crd, null);

        verFuts.put(fut.id, fut);

        try {
            cctx.gridIO().sendToGridTopic(crd,
                MSG_TOPIC,
                new CoordinatorQueryVersionRequest(fut.id),
                MSG_POLICY);
        }
        catch (IgniteCheckedException e) {
            if (verFuts.remove(fut.id) != null)
                fut.onDone(e);
        }

        return fut;
    }

    /**
     * @param crdId Coordinator ID.
     * @param txs Transaction IDs.
     * @return Future.
     */
    public IgniteInternalFuture<Void> waitTxsFuture(UUID crdId, GridLongList txs) {
        assert crdId != null;
        assert txs != null && txs.size() > 0;

        // TODO IGNITE-3478: special case for local?

        WaitAckFuture fut = new WaitAckFuture(futIdCntr.incrementAndGet(), crdId, false);

        ackFuts.put(fut.id, fut);

        try {
            cctx.gridIO().sendToGridTopic(crdId,
                MSG_TOPIC,
                new CoordinatorWaitTxsRequest(fut.id, txs),
                MSG_POLICY);
        }
        catch (ClusterTopologyCheckedException e) {
            if (ackFuts.remove(fut.id) != null)
                fut.onDone(); // No need to ack, finish without error.
        }
        catch (IgniteCheckedException e) {
            if (ackFuts.remove(fut.id) != null)
                fut.onDone(e);
        }

        return fut;
    }

    /**
     * @param crd Coordinator.
     * @param mvccVer Transaction version.
     * @return Acknowledge future.
     */
    public IgniteInternalFuture<Void> ackTxCommit(UUID crd, MvccCoordinatorVersion mvccVer) {
        assert crd != null;
        assert mvccVer != null;

        WaitAckFuture fut = new WaitAckFuture(futIdCntr.incrementAndGet(), crd, true);

        ackFuts.put(fut.id, fut);

        try {
            cctx.gridIO().sendToGridTopic(crd,
                MSG_TOPIC,
                new CoordinatorTxAckRequest(fut.id, mvccVer.counter()),
                MSG_POLICY);
        }
        catch (ClusterTopologyCheckedException e) {
            if (ackFuts.remove(fut.id) != null)
                fut.onDone(); // No need to ack, finish without error.
        }
        catch (IgniteCheckedException e) {
            if (ackFuts.remove(fut.id) != null)
                fut.onDone(e);
        }

        return fut;
    }

    /**
     * @param crdId Coordinator node ID.
     * @param mvccVer Transaction version.
     */
    public void ackTxRollback(UUID crdId, MvccCoordinatorVersion mvccVer) {
        CoordinatorTxAckRequest msg = new CoordinatorTxAckRequest(0, mvccVer.counter());

        msg.skipResponse(true);

        try {
            cctx.gridIO().sendToGridTopic(crdId,
                MSG_TOPIC,
                msg,
                MSG_POLICY);
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send tx rollback ack, node left [msg=" + msg + ", node=" + crdId + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send tx rollback ack [msg=" + msg + ", node=" + crdId + ']', e);
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Message.
     */
    private void processCoordinatorTxCounterRequest(UUID nodeId, CoordinatorTxCounterRequest msg) {
        ClusterNode node = cctx.discovery().node(nodeId);

        if (node == null) {
            if (log.isDebugEnabled())
                log.debug("Ignore tx counter request processing, node left [msg=" + msg + ", node=" + nodeId + ']');

            return;
        }

        MvccCoordinatorVersionResponse res = assignTxCounter(msg.txId(), msg.futureId());

        if (STAT_CNTRS)
            statCntrs[0].update(res.size());

        try {
            cctx.gridIO().sendToGridTopic(node,
                MSG_TOPIC,
                res,
                MSG_POLICY);
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send tx counter response, node left [msg=" + msg + ", node=" + nodeId + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send tx counter response [msg=" + msg + ", node=" + nodeId + ']', e);
        }
    }

    /**
     *
     * @param nodeId Sender node ID.
     * @param msg Message.
     */
    private void processCoordinatorQueryVersionRequest(UUID nodeId, CoordinatorQueryVersionRequest msg) {
        ClusterNode node = cctx.discovery().node(nodeId);

        if (node == null) {
            if (log.isDebugEnabled())
                log.debug("Ignore query counter request processing, node left [msg=" + msg + ", node=" + nodeId + ']');

            return;
        }

        MvccCoordinatorVersionResponse res = assignQueryCounter(nodeId, msg.futureId());

        try {
            cctx.gridIO().sendToGridTopic(node,
                MSG_TOPIC,
                res,
                MSG_POLICY);
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send query counter response, node left [msg=" + msg + ", node=" + nodeId + ']');

            onQueryDone(res.counter());
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send query counter response [msg=" + msg + ", node=" + nodeId + ']', e);

            onQueryDone(res.counter());
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Message.
     */
    private void processCoordinatorVersionResponse(UUID nodeId, MvccCoordinatorVersionResponse msg) {
        MvccVersionFuture fut = verFuts.remove(msg.futureId());

        if (fut != null) {
            if (STAT_CNTRS)
                statCntrs[1].update((System.nanoTime() - fut.startTime) * 1000);

            fut.onResponse(msg);
        }
        else {
            if (cctx.discovery().alive(nodeId))
                U.warn(log, "Failed to find query version future [node=" + nodeId + ", msg=" + msg + ']');
            else if (log.isDebugEnabled())
                log.debug("Failed to find query version future [node=" + nodeId + ", msg=" + msg + ']');
        }
    }

    /**
     * @param msg Message.
     */
    private void processCoordinatorQueryAckRequest(CoordinatorQueryAckRequest msg) {
        onQueryDone(msg.counter());
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Message.
     */
    private void processCoordinatorTxAckRequest(UUID nodeId, CoordinatorTxAckRequest msg) {
        onTxDone(msg.txCounter());

        if (STAT_CNTRS)
            statCntrs[2].update();

        if (!msg.skipResponse()) {
            try {
                cctx.gridIO().sendToGridTopic(nodeId,
                    MSG_TOPIC,
                    new CoordinatorFutureResponse(msg.futureId()),
                    MSG_POLICY);
            }
            catch (ClusterTopologyCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send tx ack response, node left [msg=" + msg + ", node=" + nodeId + ']');
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to send tx ack response [msg=" + msg + ", node=" + nodeId + ']', e);
            }
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Message.
     */
    private void processCoordinatorAckResponse(UUID nodeId, CoordinatorFutureResponse msg) {
        WaitAckFuture fut = ackFuts.remove(msg.futureId());

        if (fut != null) {
            if (STAT_CNTRS) {
                StatCounter cntr = fut.ackTx ? statCntrs[3] : statCntrs[6];

                cntr.update((System.nanoTime() - fut.startTime) * 1000);
            }

            fut.onResponse();
        }
        else {
            if (cctx.discovery().alive(nodeId))
                U.warn(log, "Failed to find tx ack future [node=" + nodeId + ", msg=" + msg + ']');
            else if (log.isDebugEnabled())
                log.debug("Failed to find tx ack future [node=" + nodeId + ", msg=" + msg + ']');
        }
    }

    /**
     * @param txId Transaction ID.
     * @return Counter.
     */
    private MvccCoordinatorVersionResponse assignTxCounter(GridCacheVersion txId, long futId) {
        assert crdVer != 0;

        long nextCtr = mvccCntr.incrementAndGet();

        // TODO IGNITE-3478 sorted? + change GridLongList.writeTo?
        MvccCoordinatorVersionResponse res = new MvccCoordinatorVersionResponse();

        for (Long txVer : activeTxs.keySet())
            res.addTx(txVer);

        Object old = activeTxs.put(nextCtr, txId);

        assert old == null : txId;

        long cleanupVer = committedCntr.get() - 1;

        for (Long qryVer : activeQueries.keySet()) {
            if (qryVer <= cleanupVer)
                cleanupVer = qryVer - 1;
        }

        res.init(futId, crdVer, nextCtr, cleanupVer);

        return res;
    }

    /**
     * @param txCntr Counter assigned to transaction.
     */
    private void onTxDone(Long txCntr) {
        GridFutureAdapter fut; // TODO IGNITE-3478.

        GridCacheVersion ver = activeTxs.remove(txCntr);

        assert ver != null;

        committedCntr.setIfGreater(txCntr);

        fut = waitTxFuts.remove(txCntr);

        if (fut != null)
            fut.onDone();
    }

    static boolean increment(AtomicInteger cntr) {
        for (;;) {
            int current = cntr.get();

            if (current == 0)
                return false;

            if (cntr.compareAndSet(current, current + 1))
                return true;
        }
    }

    /**
     * @param qryNodeId Node initiated query.
     * @return Counter for query.
     */
    private MvccCoordinatorVersionResponse assignQueryCounter(UUID qryNodeId, long futId) {
        assert crdVer != 0;

        MvccCoordinatorVersionResponse res = new MvccCoordinatorVersionResponse();

        Long mvccCntr;

        for(;;) {
            mvccCntr = committedCntr.get();

            Long trackCntr = mvccCntr;

            for (Long txVer : activeTxs.keySet()) {
                if (txVer < trackCntr)
                    trackCntr = txVer;

                res.addTx(txVer);
            }

            registerActiveQuery(trackCntr);

            if (committedCntr.get() == mvccCntr)
                break;
            else {
                res.resetTransactionsCount();

                onQueryDone(trackCntr);
            }
        }

        res.init(futId, crdVer, mvccCntr, COUNTER_NA);

        return res;
    }

    private void registerActiveQuery(Long cntr) {
        for (;;) {
            AtomicInteger qryCnt = activeQueries.get(cntr);

            if (qryCnt != null) {
                boolean inc = increment(qryCnt);

                if (!inc) {
                    activeQueries.remove(mvccCntr, qryCnt);

                    continue;
                }
            }
            else {
                qryCnt = new AtomicInteger(1);

                if (activeQueries.putIfAbsent(cntr, qryCnt) != null)
                    continue;
            }

            break;
        }
    }

    /**
     * @param mvccCntr Query counter.
     */
    private void onQueryDone(long mvccCntr) {
        AtomicInteger cntr = activeQueries.get(mvccCntr);

        assert cntr != null : mvccCntr;

        int left = cntr.decrementAndGet();

        assert left >= 0 : left;

        if (left == 0) {
            boolean rmv = activeQueries.remove(mvccCntr, cntr);

            assert rmv;
        }
    }

    /**
     * @param msg Message.
     */
    private void processCoordinatorWaitTxsRequest(final UUID nodeId, final CoordinatorWaitTxsRequest msg) {
        statCntrs[5].update();

        GridLongList txs = msg.transactions();

        GridCompoundFuture resFut = null;

        for (int i = 0; i < txs.size(); i++) {
            Long txId = txs.get(i);

            WaitTxFuture fut = waitTxFuts.get(txId);

            if (fut == null) {
                WaitTxFuture old = waitTxFuts.putIfAbsent(txId, fut = new WaitTxFuture(txId));

                if (old != null)
                    fut = old;
            }

            if (!activeTxs.containsKey(txId))
                fut.onDone();

            if (!fut.isDone()) {
                if (resFut == null)
                    resFut = new GridCompoundFuture();

                resFut.add(fut);
            }
        }

        if (resFut != null)
            resFut.markInitialized();

        if (resFut == null || resFut.isDone())
            sendFutureResponse(nodeId, msg);
        else {
            resFut.listen(new IgniteInClosure<IgniteInternalFuture>() {
                @Override public void apply(IgniteInternalFuture fut) {
                    sendFutureResponse(nodeId, msg);
                }
            });
        }
    }

    /**
     * @param nodeId
     * @param msg
     */
    private void sendFutureResponse(UUID nodeId, CoordinatorWaitTxsRequest msg) {
        try {
            cctx.gridIO().sendToGridTopic(nodeId,
                MSG_TOPIC,
                new CoordinatorFutureResponse(msg.futureId()),
                MSG_POLICY);
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send tx ack response, node left [msg=" + msg + ", node=" + nodeId + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send tx ack response [msg=" + msg + ", node=" + nodeId + ']', e);
        }
    }

    public MvccCoordinator currentCoordinator() {
        return curCrd;
    }

    public ClusterNode currentCoordinatorNode() {
        MvccCoordinator curCrd = this.curCrd;

        return curCrd != null ? curCrd.node() : null;
    }

    public MvccCoordinator currentCoordinatorForCacheAffinity(AffinityTopologyVersion topVer) {
        MvccCoordinator crd = curCrd;

        // Assert coordinator did not already change.
        assert crd == null || crd.topologyVersion().compareTo(topVer) <= 0 : crd;

        return crd;
    }

    /**
     * @param discoCache Discovery snapshot.
     */
    public MvccCoordinator reassignCoordinator(DiscoCache discoCache) {
        assert curCrd == null || !discoCache.allNodes().contains(curCrd.node()) : curCrd;

        if (!discoCache.serverNodes().isEmpty()) {
            curCrd = new MvccCoordinator(discoCache.serverNodes().get(0), discoCache.version());

            log.info("Assigned mvcc coordinator [topVer=" + discoCache.version() +
                ", crd=" + curCrd.node().id() + ']');
        }
        else
            curCrd = null;

        return curCrd;
    }

    public void initCoordinator(AffinityTopologyVersion topVer, @Nullable Map<MvccCounter, Integer> activeQrys) {
        assert cctx.localNode().equals(curCrd.node());

        log.info("Initialize local node as mvcc coordinator [node=" + cctx.localNodeId() +
            ", topVer=" + topVer + ']');

        crdVer = topVer.topologyVersion();

        crdLatch.countDown();
    }

    /**
     *
     */
    public class MvccVersionFuture extends GridFutureAdapter<MvccCoordinatorVersion> {
        /** */
        private final Long id;

        /** */
        private MvccResponseListener lsnr;

        /** */
        public final ClusterNode crd;

        /** */
        long startTime;

        /**
         * @param id Future ID.
         * @param crd Coordinator.
         */
        MvccVersionFuture(Long id, ClusterNode crd, @Nullable MvccResponseListener lsnr) {
            this.id = id;
            this.crd = crd;
            this.lsnr = lsnr;

            if (STAT_CNTRS)
                startTime = System.nanoTime();
        }

        /**
         * @param res Response.
         */
        void onResponse(MvccCoordinatorVersionResponse res) {
            assert res.counter() != COUNTER_NA;

            if (lsnr != null)
                lsnr.onMvccResponse(crd.id(), res);

            onDone(res);
        }

        void onError(IgniteCheckedException err) {
            if (verFuts.remove(id) != null) {
                if (lsnr != null)
                    lsnr.onMvccError(err);

                onDone(err);
            }
        }

        /**
         * @param nodeId Failed node ID.
         */
        void onNodeLeft(UUID nodeId) {
            if (crd.id().equals(nodeId)) {
                ClusterTopologyCheckedException err = new ClusterTopologyCheckedException("Failed to request coordinator version, " +
                    "coordinator failed: " + nodeId);

                onError(err);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "MvccVersionFuture [crd=" + crd + ", id=" + id + ']';
        }
    }

    /**
     *
     */
    private class WaitAckFuture extends GridFutureAdapter<Void> {
        /** */
        private final long id;

        /** */
        private final UUID crdId;

        /** */
        long startTime;

        /** */
        final boolean ackTx;

        /**
         * @param id Future ID.
         * @param crdId Coordinator node ID.
         */
        WaitAckFuture(long id, UUID crdId, boolean ackTx) {
            this.id = id;
            this.crdId = crdId;
            this.ackTx = ackTx;

            if (STAT_CNTRS)
                startTime = System.nanoTime();
        }

        /**
         *
         */
        void onResponse() {
            onDone();
        }

        /**
         * @param nodeId Failed node ID.
         */
        void onNodeLeft(UUID nodeId) {
            if (crdId.equals(nodeId) && verFuts.remove(id) != null)
                onDone();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "WaitAckFuture [crdId=" + crdId + ", id=" + id + ']';
        }
    }

    /**
     *
     */
    private class CacheCoordinatorDiscoveryListener implements GridLocalEventListener {
        /** {@inheritDoc} */
        @Override public void onEvent(Event evt) {
            assert evt instanceof DiscoveryEvent : evt;

            DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

            UUID nodeId = discoEvt.eventNode().id();

            for (MvccVersionFuture fut : verFuts.values())
                fut.onNodeLeft(nodeId);

            for (WaitAckFuture fut : ackFuts.values())
                fut.onNodeLeft(nodeId);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "CacheCoordinatorDiscoveryListener[]";
        }
    }
    /**
     *
     */
    private class CoordinatorMessageListener implements GridMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
            if (STAT_CNTRS)
                statCntrs[4].update();

            MvccCoordinatorMessage msg0 = (MvccCoordinatorMessage)msg;

            if (msg0.waitForCoordinatorInit()) {
                if (crdVer == 0) {
                    try {
                        U.await(crdLatch);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        U.warn(log, "Failed to wait for coordinator initialization, thread interrupted [" +
                            "msgNode=" + nodeId + ", msg=" + msg + ']');

                        return;
                    }

                    assert crdVer != 0L;
                }
            }

            if (msg instanceof CoordinatorTxCounterRequest)
                processCoordinatorTxCounterRequest(nodeId, (CoordinatorTxCounterRequest)msg);
            else if (msg instanceof CoordinatorTxAckRequest)
                processCoordinatorTxAckRequest(nodeId, (CoordinatorTxAckRequest)msg);
            else if (msg instanceof CoordinatorFutureResponse)
                processCoordinatorAckResponse(nodeId, (CoordinatorFutureResponse)msg);
            else if (msg instanceof CoordinatorQueryAckRequest)
                processCoordinatorQueryAckRequest((CoordinatorQueryAckRequest)msg);
            else if (msg instanceof CoordinatorQueryVersionRequest)
                processCoordinatorQueryVersionRequest(nodeId, (CoordinatorQueryVersionRequest)msg);
            else if (msg instanceof MvccCoordinatorVersionResponse)
                processCoordinatorVersionResponse(nodeId, (MvccCoordinatorVersionResponse) msg);
            else if (msg instanceof CoordinatorWaitTxsRequest)
                processCoordinatorWaitTxsRequest(nodeId, (CoordinatorWaitTxsRequest)msg);
            else
                U.warn(log, "Unexpected message received [node=" + nodeId + ", msg=" + msg + ']');
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "CoordinatorMessageListener[]";
        }
    }
    /**
     *
     */
    static class StatCounter {
        /** */
        final String name;

        /** */
        final LongAdder8 cntr = new LongAdder8();

        public StatCounter(String name) {
            this.name = name;
        }

        void update() {
            cntr.increment();
        }

        void update(GridLongList arg) {
            throw new UnsupportedOperationException();
        }

        void update(long arg) {
            throw new UnsupportedOperationException();
        }

        void dumpInfo(IgniteLogger log) {
            long totalCnt = cntr.sumThenReset();

            if (totalCnt > 0)
                log.info(name + " [cnt=" + totalCnt + ']');
        }
    }

    /**
     *
     */
    static class CounterWithAvg extends StatCounter {
        /** */
        final LongAdder8 total = new LongAdder8();

        /** */
        final String avgName;

        CounterWithAvg(String name, String avgName) {
            super(name);

            this.avgName = avgName;
        }

        @Override void update(GridLongList arg) {
            update(arg != null ? arg.size() : 0);
        }

        @Override void update(long add) {
            cntr.increment();

            total.add(add);
        }

        void dumpInfo(IgniteLogger log) {
            long totalCnt = cntr.sumThenReset();
            long totalSum = total.sumThenReset();

            if (totalCnt > 0)
                log.info(name + " [cnt=" + totalCnt + ", " + avgName + "=" + ((float)totalSum / totalCnt) + ']');
        }
    }

    /**
     *
     */
    private static class WaitTxFuture extends GridFutureAdapter {
        /** */
        private final long txId;

        /**
         * @param txId Transaction ID.
         */
        WaitTxFuture(long txId) {
            this.txId = txId;
        }
    }
}
