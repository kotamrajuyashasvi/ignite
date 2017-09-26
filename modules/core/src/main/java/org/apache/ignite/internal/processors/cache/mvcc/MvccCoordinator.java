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

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;

/**
 *
 */
public class MvccCoordinator {
    /** */
    private final ClusterNode crd;

    /** */
    private final AffinityTopologyVersion topVer;

    public MvccCoordinator(ClusterNode crd, final AffinityTopologyVersion topVer) {
        this.crd = crd;
        this.topVer = topVer;
    }

    public ClusterNode node() {
        return crd;
    }

    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    @Override public boolean equals(Object other) {
        if (this == other)
            return true;

        if (other == null || getClass() != other.getClass())
            return false;

        MvccCoordinator that = (MvccCoordinator)other;

        return topVer.equals(topVer) && crd.equals(that.crd);
    }

    @Override public int hashCode() {
        int res = crd.hashCode();

        res = 31 * res + topVer.hashCode();

        return res;
    }
}
