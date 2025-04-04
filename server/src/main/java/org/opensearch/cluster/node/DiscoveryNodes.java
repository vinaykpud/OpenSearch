/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.node;

import org.opensearch.Version;
import org.opensearch.cluster.AbstractDiffable;
import org.opensearch.cluster.Diff;
import org.opensearch.common.Booleans;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.BufferedChecksumStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.VerifiableWriteable;
import org.opensearch.core.common.transport.TransportAddress;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This class holds all {@link DiscoveryNode} in the cluster and provides convenience methods to
 * access, modify merge / diff discovery nodes.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class DiscoveryNodes extends AbstractDiffable<DiscoveryNodes> implements Iterable<DiscoveryNode>, VerifiableWriteable {

    public static final DiscoveryNodes EMPTY_NODES = builder().build();

    private final Map<String, DiscoveryNode> nodes;
    private final Map<String, DiscoveryNode> dataNodes;
    private final Map<String, DiscoveryNode> clusterManagerNodes;
    private final Map<String, DiscoveryNode> ingestNodes;

    private final String clusterManagerNodeId;
    private final String localNodeId;
    private final Version minNonClientNodeVersion;
    private final Version maxNonClientNodeVersion;
    private final Version maxNodeVersion;
    private final Version minNodeVersion;

    private DiscoveryNodes(
        final Map<String, DiscoveryNode> nodes,
        final Map<String, DiscoveryNode> dataNodes,
        final Map<String, DiscoveryNode> clusterManagerNodes,
        final Map<String, DiscoveryNode> ingestNodes,
        String clusterManagerNodeId,
        String localNodeId,
        Version minNonClientNodeVersion,
        Version maxNonClientNodeVersion,
        Version maxNodeVersion,
        Version minNodeVersion
    ) {
        this.nodes = Collections.unmodifiableMap(nodes);
        this.dataNodes = Collections.unmodifiableMap(dataNodes);
        this.clusterManagerNodes = Collections.unmodifiableMap(clusterManagerNodes);
        this.ingestNodes = Collections.unmodifiableMap(ingestNodes);
        this.clusterManagerNodeId = clusterManagerNodeId;
        this.localNodeId = localNodeId;
        this.minNonClientNodeVersion = minNonClientNodeVersion;
        this.maxNonClientNodeVersion = maxNonClientNodeVersion;
        this.minNodeVersion = minNodeVersion;
        this.maxNodeVersion = maxNodeVersion;
    }

    @Override
    public Iterator<DiscoveryNode> iterator() {
        return nodes.values().iterator();
    }

    /**
     * Returns {@code true} if the local node is the elected cluster-manager node.
     */
    public boolean isLocalNodeElectedClusterManager() {
        if (localNodeId == null) {
            // we don't know yet the local node id, return false
            return false;
        }
        return localNodeId.equals(clusterManagerNodeId);
    }

    /**
     * Get the number of known nodes
     *
     * @return number of nodes
     */
    public int getSize() {
        return nodes.size();
    }

    /**
     * Get a {@link Map} of the discovered nodes arranged by their ids
     *
     * @return {@link Map} of the discovered nodes arranged by their ids
     */
    public Map<String, DiscoveryNode> getNodes() {
        return this.nodes;
    }

    /**
     * Get a {@link Map} of the discovered data nodes arranged by their ids
     *
     * @return {@link Map} of the discovered data nodes arranged by their ids
     */
    public Map<String, DiscoveryNode> getDataNodes() {
        return this.dataNodes;
    }

    /**
     * Get a {@link Map} of the discovered cluster-manager nodes arranged by their ids
     *
     * @return {@link Map} of the discovered cluster-manager nodes arranged by their ids
     */
    public Map<String, DiscoveryNode> getClusterManagerNodes() {
        return this.clusterManagerNodes;
    }

    /**
     * @return All the ingest nodes arranged by their ids
     */
    public Map<String, DiscoveryNode> getIngestNodes() {
        return ingestNodes;
    }

    /**
     * Get a {@link Map} of the discovered cluster-manager and data nodes arranged by their ids
     *
     * @return {@link Map} of the discovered cluster-manager and data nodes arranged by their ids
     */
    public Map<String, DiscoveryNode> getClusterManagerAndDataNodes() {
        final Map<String, DiscoveryNode> nodes = new HashMap<>(dataNodes);
        nodes.putAll(clusterManagerNodes);
        return Collections.unmodifiableMap(nodes);
    }

    /**
     * Get a {@link Map} of the coordinating only nodes (nodes which are neither cluster-manager, nor data, nor ingest nodes) arranged by their ids
     *
     * @return {@link Map} of the coordinating only nodes arranged by their ids
     */
    public Map<String, DiscoveryNode> getCoordinatingOnlyNodes() {
        final HashMap<String, DiscoveryNode> nodes = new HashMap<>(this.nodes);
        nodes.keySet().removeAll(clusterManagerNodes.keySet());
        nodes.keySet().removeAll(dataNodes.keySet());
        nodes.keySet().removeAll(ingestNodes.keySet());
        return Collections.unmodifiableMap(nodes);
    }

    /**
     * Returns a stream of all nodes, with cluster-manager nodes at the front
     */
    public Stream<DiscoveryNode> clusterManagersFirstStream() {
        return Stream.concat(
            StreamSupport.stream(Spliterators.spliterator(clusterManagerNodes.entrySet(), 0), false).map(cur -> cur.getValue()),
            StreamSupport.stream(this.spliterator(), false).filter(n -> n.isClusterManagerNode() == false)
        );
    }

    /**
     * Get a node by its id
     *
     * @param nodeId id of the wanted node
     * @return wanted node if it exists. Otherwise <code>null</code>
     */
    public DiscoveryNode get(String nodeId) {
        return nodes.get(nodeId);
    }

    /**
     * Determine if a given node id exists
     *
     * @param nodeId id of the node which existence should be verified
     * @return <code>true</code> if the node exists. Otherwise <code>false</code>
     */
    public boolean nodeExists(String nodeId) {
        return nodes.containsKey(nodeId);
    }

    /**
     * Determine if a given node exists
     *
     * @param node of the node which existence should be verified
     * @return <code>true</code> if the node exists. Otherwise <code>false</code>
     */
    public boolean nodeExists(DiscoveryNode node) {
        DiscoveryNode existing = nodes.get(node.getId());
        return existing != null && existing.equals(node);
    }

    /**
     * Determine if the given node exists and has the right roles. Supported roles vary by version, and our local cluster state might
     * have come via an older cluster-manager, so the roles may differ even if the node is otherwise identical.
     */
    public boolean nodeExistsWithSameRoles(DiscoveryNode discoveryNode) {
        final DiscoveryNode existing = nodes.get(discoveryNode.getId());
        return existing != null && existing.equals(discoveryNode) && existing.getRoles().equals(discoveryNode.getRoles());
    }

    /**
     * Get the id of the cluster-manager node
     *
     * @return id of the cluster-manager
     */
    public String getClusterManagerNodeId() {
        return this.clusterManagerNodeId;
    }

    /**
     * Get the id of the local node
     *
     * @return id of the local node
     */
    public String getLocalNodeId() {
        return this.localNodeId;
    }

    /**
     * Get the local node
     *
     * @return local node
     */
    public DiscoveryNode getLocalNode() {
        return nodes.get(localNodeId);
    }

    /**
     * Returns the cluster-manager node, or {@code null} if there is no cluster-manager node
     */
    @Nullable
    public DiscoveryNode getClusterManagerNode() {
        if (clusterManagerNodeId != null) {
            return nodes.get(clusterManagerNodeId);
        }
        return null;
    }

    /**
     * Get a node by its address
     *
     * @param address {@link TransportAddress} of the wanted node
     * @return node identified by the given address or <code>null</code> if no such node exists
     */
    public DiscoveryNode findByAddress(TransportAddress address) {
        for (final DiscoveryNode node : nodes.values()) {
            if (node.getAddress().equals(address)) {
                return node;
            }
        }
        return null;
    }

    /**
     * Returns the version of the node with the oldest version in the cluster that is not a client node
     * <p>
     * If there are no non-client nodes, Version.CURRENT will be returned.
     *
     * @return the oldest version in the cluster
     */
    public Version getSmallestNonClientNodeVersion() {
        return minNonClientNodeVersion;
    }

    /**
     * Returns the version of the node with the youngest version in the cluster that is not a client node.
     * <p>
     * If there are no non-client nodes, Version.CURRENT will be returned.
     *
     * @return the youngest version in the cluster
     */
    public Version getLargestNonClientNodeVersion() {
        return maxNonClientNodeVersion;
    }

    /**
     * Returns the version of the node with the oldest version in the cluster.
     *
     * @return the oldest version in the cluster
     */
    public Version getMinNodeVersion() {
        return minNodeVersion;
    }

    /**
     * Returns the version of the node with the youngest version in the cluster
     *
     * @return the youngest version in the cluster
     */
    public Version getMaxNodeVersion() {
        return maxNodeVersion;
    }

    /**
     * Resolve a node with a given id
     *
     * @param node id of the node to discover
     * @return discovered node matching the given id
     * @throws IllegalArgumentException if more than one node matches the request or no nodes have been resolved
     */
    public DiscoveryNode resolveNode(String node) {
        String[] resolvedNodeIds = resolveNodes(node);
        if (resolvedNodeIds.length > 1) {
            throw new IllegalArgumentException(
                "resolved [" + node + "] into [" + resolvedNodeIds.length + "] nodes, where expected to be resolved to a single node"
            );
        }
        if (resolvedNodeIds.length == 0) {
            throw new IllegalArgumentException("failed to resolve [" + node + "], no matching nodes");
        }
        return nodes.get(resolvedNodeIds[0]);
    }

    /**
     * Resolves a set of nodes according to the given sequence of node specifications. Implements the logic in various APIs that allow the
     * user to run the action on a subset of the nodes in the cluster. See [Node specification] in the reference manual for full details.
     * <p>
     * Works by tracking the current set of nodes and applying each node specification in sequence. The set starts out empty and each node
     * specification may either add or remove nodes. For instance:
     * <p>
     * - _local, _cluster_manager (_master) and _all respectively add to the subset the local node, the currently-elected cluster_manager, and all the nodes
     * - node IDs, names, hostnames and IP addresses all add to the subset any nodes which match
     * - a wildcard-based pattern of the form "attr*:value*" adds to the subset all nodes with a matching attribute with a matching value
     * - role:true adds to the subset all nodes with a matching role
     * - role:false removes from the subset all nodes with a matching role.
     * <p>
     * An empty sequence of node specifications returns all nodes, since the corresponding actions run on all nodes by default.
     */
    public String[] resolveNodes(String... nodes) {
        if (nodes == null || nodes.length == 0) {
            return StreamSupport.stream(this.spliterator(), false).map(DiscoveryNode::getId).toArray(String[]::new);
        } else {
            final HashSet<String> resolvedNodesIds = new HashSet<>(nodes.length);
            for (String nodeId : nodes) {
                if (nodeId == null) {
                    // don't silence the underlying issue, it is a bug, so lets fail if assertions are enabled
                    assert nodeId != null : "nodeId should not be null";
                    continue;
                } else if (nodeId.equals("_local")) {
                    String localNodeId = getLocalNodeId();
                    if (localNodeId != null) {
                        resolvedNodesIds.add(localNodeId);
                    }
                } else if (nodeId.equals("_master") || nodeId.equals("_cluster_manager")) {
                    String clusterManagerNodeId = getClusterManagerNodeId();
                    if (clusterManagerNodeId != null) {
                        resolvedNodesIds.add(clusterManagerNodeId);
                    }
                } else if (nodeExists(nodeId)) {
                    resolvedNodesIds.add(nodeId);
                } else {
                    for (DiscoveryNode node : this) {
                        if ("_all".equals(nodeId)
                            || Regex.simpleMatch(nodeId, node.getName())
                            || Regex.simpleMatch(nodeId, node.getHostAddress())
                            || Regex.simpleMatch(nodeId, node.getHostName())) {
                            resolvedNodesIds.add(node.getId());
                        }
                    }
                    int index = nodeId.indexOf(':');
                    if (index != -1) {
                        String matchAttrName = nodeId.substring(0, index);
                        String matchAttrValue = nodeId.substring(index + 1);
                        if (DiscoveryNodeRole.DATA_ROLE.roleName().equals(matchAttrName)) {
                            if (Booleans.parseBoolean(matchAttrValue, true)) {
                                resolvedNodesIds.addAll(dataNodes.keySet());
                            } else {
                                resolvedNodesIds.removeAll(dataNodes.keySet());
                            }
                        } else if (roleNameIsClusterManager(matchAttrName)) {
                            if (Booleans.parseBoolean(matchAttrValue, true)) {
                                resolvedNodesIds.addAll(clusterManagerNodes.keySet());
                            } else {
                                resolvedNodesIds.removeAll(clusterManagerNodes.keySet());
                            }
                        } else if (DiscoveryNodeRole.INGEST_ROLE.roleName().equals(matchAttrName)) {
                            if (Booleans.parseBoolean(matchAttrValue, true)) {
                                resolvedNodesIds.addAll(ingestNodes.keySet());
                            } else {
                                resolvedNodesIds.removeAll(ingestNodes.keySet());
                            }
                        } else if (DiscoveryNode.COORDINATING_ONLY.equals(matchAttrName)) {
                            if (Booleans.parseBoolean(matchAttrValue, true)) {
                                resolvedNodesIds.addAll(getCoordinatingOnlyNodes().keySet());
                            } else {
                                resolvedNodesIds.removeAll(getCoordinatingOnlyNodes().keySet());
                            }
                        } else {
                            for (DiscoveryNode node : this) {
                                for (DiscoveryNodeRole role : Sets.difference(node.getRoles(), DiscoveryNodeRole.BUILT_IN_ROLES)) {
                                    if (role.roleName().equals(matchAttrName)) {
                                        if (Booleans.parseBoolean(matchAttrValue, true)) {
                                            resolvedNodesIds.add(node.getId());
                                        } else {
                                            resolvedNodesIds.remove(node.getId());
                                        }
                                    }
                                }
                            }
                            for (DiscoveryNode node : this) {
                                for (Map.Entry<String, String> entry : node.getAttributes().entrySet()) {
                                    String attrName = entry.getKey();
                                    String attrValue = entry.getValue();
                                    if (Regex.simpleMatch(matchAttrName, attrName) && Regex.simpleMatch(matchAttrValue, attrValue)) {
                                        resolvedNodesIds.add(node.getId());
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return resolvedNodesIds.toArray(new String[0]);
        }
    }

    public DiscoveryNodes newNode(DiscoveryNode node) {
        return new Builder(this).add(node).build();
    }

    /**
     * Returns the changes comparing this nodes to the provided nodes.
     */
    public Delta delta(DiscoveryNodes other) {
        final List<DiscoveryNode> removed = new ArrayList<>();
        final List<DiscoveryNode> added = new ArrayList<>();
        for (DiscoveryNode node : other) {
            if (this.nodeExists(node) == false) {
                removed.add(node);
            }
        }
        for (DiscoveryNode node : this) {
            if (other.nodeExists(node) == false) {
                added.add(node);
            }
        }

        return new Delta(
            other.getClusterManagerNode(),
            getClusterManagerNode(),
            localNodeId,
            Collections.unmodifiableList(removed),
            Collections.unmodifiableList(added)
        );
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("nodes: \n");
        for (DiscoveryNode node : this) {
            sb.append("   ").append(node);
            if (node == getLocalNode()) {
                sb.append(", local");
            }
            if (node == getClusterManagerNode()) {
                sb.append(", cluster-manager");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    /**
     * Delta between nodes.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Delta {

        private final String localNodeId;
        @Nullable
        private final DiscoveryNode previousClusterManagerNode;
        @Nullable
        private final DiscoveryNode newClusterManagerNode;
        private final List<DiscoveryNode> removed;
        private final List<DiscoveryNode> added;

        private Delta(
            @Nullable DiscoveryNode previousClusterManagerNode,
            @Nullable DiscoveryNode newClusterManagerNode,
            String localNodeId,
            List<DiscoveryNode> removed,
            List<DiscoveryNode> added
        ) {
            this.previousClusterManagerNode = previousClusterManagerNode;
            this.newClusterManagerNode = newClusterManagerNode;
            this.localNodeId = localNodeId;
            this.removed = removed;
            this.added = added;
        }

        public boolean hasChanges() {
            return clusterManagerNodeChanged() || !removed.isEmpty() || !added.isEmpty();
        }

        public boolean clusterManagerNodeChanged() {
            return Objects.equals(newClusterManagerNode, previousClusterManagerNode) == false;
        }

        @Nullable
        public DiscoveryNode previousClusterManagerNode() {
            return previousClusterManagerNode;
        }

        @Nullable
        public DiscoveryNode newClusterManagerNode() {
            return newClusterManagerNode;
        }

        public boolean removed() {
            return !removed.isEmpty();
        }

        public List<DiscoveryNode> removedNodes() {
            return removed;
        }

        public boolean added() {
            return !added.isEmpty();
        }

        public List<DiscoveryNode> addedNodes() {
            return added;
        }

        public String shortSummary() {
            final StringBuilder summary = new StringBuilder();
            if (clusterManagerNodeChanged()) {
                summary.append("cluster-manager node changed {previous [");
                if (previousClusterManagerNode() != null) {
                    summary.append(previousClusterManagerNode());
                }
                summary.append("], current [");
                if (newClusterManagerNode() != null) {
                    summary.append(newClusterManagerNode());
                }
                summary.append("]}");
            }
            if (removed()) {
                if (summary.length() > 0) {
                    summary.append(", ");
                }
                summary.append("removed {").append(Strings.collectionToCommaDelimitedString(removedNodes())).append('}');
            }
            if (added()) {
                final String addedNodesExceptLocalNode = addedNodes().stream()
                    .filter(node -> node.getId().equals(localNodeId) == false)
                    .map(DiscoveryNode::toString)
                    .collect(Collectors.joining(","));
                if (addedNodesExceptLocalNode.length() > 0) {
                    // ignore ourselves when reporting on nodes being added
                    if (summary.length() > 0) {
                        summary.append(", ");
                    }
                    summary.append("added {").append(addedNodesExceptLocalNode).append('}');
                }
            }
            return summary.toString();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeToUtil((output, value) -> value.writeTo(output), out);
    }

    public void writeToWithAttribute(StreamOutput out) throws IOException {
        writeToUtil((output, value) -> value.writeToWithAttribute(output), out);
    }

    public void writeToUtil(final Writer<DiscoveryNode> writer, StreamOutput out) throws IOException {
        writeClusterManager(out);
        out.writeVInt(nodes.size());
        for (DiscoveryNode node : this) {
            writer.write(out, node);
        }
    }

    @Override
    public void writeVerifiableTo(BufferedChecksumStreamOutput out) throws IOException {
        writeClusterManager(out);
        out.writeMapValues(nodes, (stream, val) -> val.writeVerifiableTo((BufferedChecksumStreamOutput) stream));
    }

    private void writeClusterManager(StreamOutput out) throws IOException {
        if (clusterManagerNodeId == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeString(clusterManagerNodeId);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DiscoveryNodes that = (DiscoveryNodes) o;
        return Objects.equals(nodes, that.nodes)
            && Objects.equals(dataNodes, that.dataNodes)
            && Objects.equals(clusterManagerNodes, that.clusterManagerNodes)
            && Objects.equals(ingestNodes, that.ingestNodes)
            && Objects.equals(clusterManagerNodeId, that.clusterManagerNodeId)
            && Objects.equals(localNodeId, that.localNodeId)
            && Objects.equals(minNonClientNodeVersion, that.minNonClientNodeVersion)
            && Objects.equals(maxNonClientNodeVersion, that.maxNonClientNodeVersion)
            && Objects.equals(maxNodeVersion, that.maxNodeVersion)
            && Objects.equals(minNodeVersion, that.minNodeVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            nodes,
            dataNodes,
            clusterManagerNodes,
            ingestNodes,
            clusterManagerNodeId,
            localNodeId,
            minNonClientNodeVersion,
            maxNonClientNodeVersion,
            maxNodeVersion,
            minNodeVersion
        );
    }

    public static DiscoveryNodes readFrom(StreamInput in, DiscoveryNode localNode) throws IOException {
        Builder builder = new Builder();
        if (in.readBoolean()) {
            builder.clusterManagerNodeId(in.readString());
        }
        if (localNode != null) {
            builder.localNodeId(localNode.getId());
        }
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            DiscoveryNode node = new DiscoveryNode(in);
            if (localNode != null && node.getId().equals(localNode.getId())) {
                // reuse the same instance of our address and local node id for faster equality
                node = localNode;
            }
            // some one already built this and validated it's OK, skip the n2 scans
            assert builder.validateAdd(node) == null : "building disco nodes from network doesn't pass preflight: "
                + builder.validateAdd(node);
            builder.putUnsafe(node);
        }
        return builder.build();
    }

    public static Diff<DiscoveryNodes> readDiffFrom(StreamInput in, DiscoveryNode localNode) throws IOException {
        return AbstractDiffable.readDiffFrom(in1 -> readFrom(in1, localNode), in);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(DiscoveryNodes nodes) {
        return new Builder(nodes);
    }

    /**
     * Builder of a map of discovery nodes.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Builder {

        private final Map<String, DiscoveryNode> nodes;
        private String clusterManagerNodeId;
        private String localNodeId;

        public Builder() {
            nodes = new HashMap<>();
        }

        public Builder(DiscoveryNodes nodes) {
            this.clusterManagerNodeId = nodes.getClusterManagerNodeId();
            this.localNodeId = nodes.getLocalNodeId();
            this.nodes = new HashMap<>(nodes.getNodes());
        }

        /**
         * adds a disco node to the builder. Will throw an {@link IllegalArgumentException} if
         * the supplied node doesn't pass the pre-flight checks performed by {@link #validateAdd(DiscoveryNode)}
         */
        public Builder add(DiscoveryNode node) {
            final String preflight = validateAdd(node);
            if (preflight != null) {
                throw new IllegalArgumentException(preflight);
            }
            putUnsafe(node);
            return this;
        }

        /**
         * Get a node by its id
         *
         * @param nodeId id of the wanted node
         * @return wanted node if it exists. Otherwise <code>null</code>
         */
        @Nullable
        public DiscoveryNode get(String nodeId) {
            return nodes.get(nodeId);
        }

        private void putUnsafe(DiscoveryNode node) {
            nodes.put(node.getId(), node);
        }

        public Builder remove(String nodeId) {
            nodes.remove(nodeId);
            return this;
        }

        public Builder remove(DiscoveryNode node) {
            if (node.equals(nodes.get(node.getId()))) {
                nodes.remove(node.getId());
            }
            return this;
        }

        public Builder clusterManagerNodeId(String clusterManagerNodeId) {
            this.clusterManagerNodeId = clusterManagerNodeId;
            return this;
        }

        public Builder localNodeId(String localNodeId) {
            this.localNodeId = localNodeId;
            return this;
        }

        /**
         * Checks that a node can be safely added to this node collection.
         *
         * @return null if all is OK or an error message explaining why a node can not be added.
         * <p>
         * Note: if this method returns a non-null value, calling {@link #add(DiscoveryNode)} will fail with an
         * exception
         */
        private String validateAdd(DiscoveryNode node) {
            for (final DiscoveryNode existingNode : nodes.values()) {
                if (node.getAddress().equals(existingNode.getAddress()) && node.getId().equals(existingNode.getId()) == false) {
                    return "can't add node " + node + ", found existing node " + existingNode + " with same address";
                }
                if (node.getId().equals(existingNode.getId()) && node.equals(existingNode) == false) {
                    return "can't add node "
                        + node
                        + ", found existing node "
                        + existingNode
                        + " with the same id but is a different node instance";
                }
            }
            return null;
        }

        public DiscoveryNodes build() {
            final Map<String, DiscoveryNode> dataNodesBuilder = new HashMap<>();
            final Map<String, DiscoveryNode> clusterManagerNodesBuilder = new HashMap<>();
            final Map<String, DiscoveryNode> ingestNodesBuilder = new HashMap<>();
            Version minNodeVersion = null;
            Version maxNodeVersion = null;
            Version minNonClientNodeVersion = null;
            Version maxNonClientNodeVersion = null;
            for (final Map.Entry<String, DiscoveryNode> nodeEntry : nodes.entrySet()) {
                if (nodeEntry.getValue().isDataNode()) {
                    dataNodesBuilder.put(nodeEntry.getKey(), nodeEntry.getValue());
                }
                if (nodeEntry.getValue().isClusterManagerNode()) {
                    clusterManagerNodesBuilder.put(nodeEntry.getKey(), nodeEntry.getValue());
                }
                final Version version = nodeEntry.getValue().getVersion();
                if (nodeEntry.getValue().isDataNode() || nodeEntry.getValue().isClusterManagerNode()) {
                    if (minNonClientNodeVersion == null) {
                        minNonClientNodeVersion = version;
                        maxNonClientNodeVersion = version;
                    } else {
                        minNonClientNodeVersion = Version.min(minNonClientNodeVersion, version);
                        maxNonClientNodeVersion = Version.max(maxNonClientNodeVersion, version);
                    }
                }
                if (nodeEntry.getValue().isIngestNode()) {
                    ingestNodesBuilder.put(nodeEntry.getKey(), nodeEntry.getValue());
                }
                minNodeVersion = minNodeVersion == null ? version : Version.min(minNodeVersion, version);
                maxNodeVersion = maxNodeVersion == null ? version : Version.max(maxNodeVersion, version);
            }

            return new DiscoveryNodes(
                nodes,
                dataNodesBuilder,
                clusterManagerNodesBuilder,
                ingestNodesBuilder,
                clusterManagerNodeId,
                localNodeId,
                minNonClientNodeVersion == null ? Version.CURRENT : minNonClientNodeVersion,
                maxNonClientNodeVersion == null ? Version.CURRENT : maxNonClientNodeVersion,
                maxNodeVersion == null ? Version.CURRENT : maxNodeVersion,
                minNodeVersion == null ? Version.CURRENT : minNodeVersion
            );
        }

        public boolean isLocalNodeElectedClusterManager() {
            return clusterManagerNodeId != null && clusterManagerNodeId.equals(localNodeId);
        }
    }

    /**
     * Check if the given name of the node role is 'cluster_manager' or 'master'.
     * The method is added for {@link #resolveNodes} to keep the code clear, when support the both above roles.
     * @deprecated As of 2.0, because promoting inclusive language. MASTER_ROLE is deprecated.
     * @param matchAttrName a given String for a name of the node role.
     * @return true if the given roleName is 'cluster_manger' or 'master'
     */
    @Deprecated
    private boolean roleNameIsClusterManager(String matchAttrName) {
        return DiscoveryNodeRole.MASTER_ROLE.roleName().equals(matchAttrName)
            || DiscoveryNodeRole.CLUSTER_MANAGER_ROLE.roleName().equals(matchAttrName);
    }
}
