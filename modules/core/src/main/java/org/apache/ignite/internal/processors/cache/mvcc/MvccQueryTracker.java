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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class MvccQueryTracker {
    /** */
    private MvccCoordinator mvccCrd;

    /** */
    private MvccCoordinatorVersion mvccVer;

    /** */
    private final GridCacheContext cctx;

    /** */
    private final boolean canRemap;

    /** */
    private final MvccQueryAware lsnr;

    /**
     * @param cctx
     * @param canRemap
     * @param lsnr
     */
    public MvccQueryTracker(GridCacheContext cctx, boolean canRemap, MvccQueryAware lsnr) {
        assert cctx.mvccEnabled() : cctx.name();

        this.cctx = cctx;
        this.canRemap = canRemap;
        this.lsnr = lsnr;
    }

    @Nullable public MvccCoordinatorVersion onMvccCoordinatorChange(MvccCoordinator newCrd) {
        synchronized (this) {
            if (mvccVer != null) {
                mvccCrd = newCrd;

                return mvccVer;
            }
            else if (mvccCrd != null)
                mvccCrd = null;

            return null;
        }
    }

    public MvccCoordinatorVersion mvccVersion() {
        return mvccVer;
    }

    public void onQueryDone() {
        MvccCoordinator mvccCrd0 = null;
        MvccCoordinatorVersion mvccVer0 = null;

        synchronized (this) {
            if (mvccVer != null) {
                assert mvccCrd != null;

                mvccCrd0 = mvccCrd;
                mvccVer0 = mvccVer;

                mvccVer = null;
            }
        }

        if (mvccVer0 != null)
            cctx.shared().coordinators().ackQueryDone(mvccCrd0, mvccVer0);
    }

    public void requestVersion(final AffinityTopologyVersion topVer) {
        MvccCoordinator mvccCrd0 = cctx.affinity().mvccCoordinator(topVer);

        if (mvccCrd0 == null) {
            lsnr.onMvccVersionError(new IgniteCheckedException("Mvcc coordinator is not assigned: " + topVer));

            return;
        }

        synchronized (this) {
            this.mvccCrd = mvccCrd0;
        }

        MvccCoordinator curCrd = cctx.topology().mvccCoordinator();

        if (!mvccCrd0.equals(curCrd)) {
            assert cctx.topology().topologyVersionFuture().initialVersion().compareTo(topVer) > 0;

            if (!canRemap) {
                lsnr.onMvccVersionError(new ClusterTopologyCheckedException("Failed to request mvcc version, coordinator changed."));

                return;
            }
            else
                waitNextTopology(topVer);
        }

        IgniteInternalFuture<MvccCoordinatorVersion> cntrFut =
            cctx.shared().coordinators().requestQueryCounter(mvccCrd);

        cntrFut.listen(new IgniteInClosure<IgniteInternalFuture<MvccCoordinatorVersion>>() {
            @Override public void apply(IgniteInternalFuture<MvccCoordinatorVersion> fut) {
                try {
                    MvccCoordinatorVersion rcvdVer = fut.get();

                    boolean needRemap = false;

                    synchronized (MvccQueryTracker.this) {
                        if (mvccCrd != null)
                            mvccVer = rcvdVer;
                        else
                            needRemap = true;
                    }

                    if (!needRemap) {
                        lsnr.onMvccVersionReceived(topVer);

                        return;
                    }
                }
                catch (ClusterTopologyCheckedException e) {
                    IgniteLogger log = cctx.logger(MvccQueryTracker.class);

                    if (log.isDebugEnabled())
                        log.debug("Mvcc coordinator failed: " + e);
                }
                catch (IgniteCheckedException e) {
                    lsnr.onMvccVersionError(e);

                    return;
                }

                // Coordinator failed on reassigned, need remap.
                if (canRemap)
                    waitNextTopology(topVer);
                else {
                    lsnr.onMvccVersionError(new ClusterTopologyCheckedException("Failed to " +
                        "request mvcc version, coordinator failed."));
                }
            }
        });
    }

    private void waitNextTopology(AffinityTopologyVersion topVer) {
        assert canRemap;

        IgniteInternalFuture<AffinityTopologyVersion> waitFut =
            cctx.shared().exchange().affinityReadyFuture(topVer.nextMinorVersion());

        if (waitFut == null)
            requestVersion(cctx.shared().exchange().readyAffinityVersion());
        else {
            waitFut.listen(new IgniteInClosure<IgniteInternalFuture<AffinityTopologyVersion>>() {
                @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> fut) {
                    try {
                        requestVersion(fut.get());
                    }
                    catch (IgniteCheckedException e) {
                        lsnr.onMvccVersionError(e);
                    }
                }
            });
        }
    }
}
