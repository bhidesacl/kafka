/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.processor.internals;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.internals.Refac_TopicStore.StateStoreFactory;
import org.apache.kafka.streams.processor.internals.nf.NodeFactory;
import org.apache.kafka.streams.processor.internals.nf.SourceNodeFactory;
import org.apache.kafka.streams.state.StoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalTopologyBuilder {

	private static final Logger log = LoggerFactory.getLogger(InternalTopologyBuilder.class);

	// node factories in a topological order
	private final Map<String, NodeFactory> nodeFactories = new LinkedHashMap<>();

	private final Refac_TopicStore topicStore;

	private final Refac_SourceSink sourceSink;

	private final Refac_TopicPatterns topicPatterns;

	private final Refac_GlobalTopics globalTopics = new Refac_GlobalTopics();

	private final QuickUnion<String> nodeGrouper = new QuickUnion<>();

	private SubscriptionUpdates subscriptionUpdates = new SubscriptionUpdates();

	private String applicationId = null;

	private Pattern topicPattern = null;

	private final static NodeComparator NODE_COMPARATOR = new NodeComparator();

	private final static GlobalStoreComparator GLOBALSTORE_COMPARATOR = new GlobalStoreComparator();

	private final static SubtopologyComparator SUBTOPOLOGY_COMPARATOR = new SubtopologyComparator();

	public InternalTopologyBuilder() {
		this.sourceSink = new Refac_SourceSink(nodeFactories, subscriptionUpdates, globalTopics, nodeGrouper);
		this.topicStore = new Refac_TopicStore(sourceSink, globalTopics, nodeGrouper, nodeFactories);
		this.topicPatterns = new Refac_TopicPatterns(nodeFactories, sourceSink, nodeGrouper, topicStore, globalTopics);
	}

	// public for testing only
	public synchronized final InternalTopologyBuilder setApplicationId(final String applicationId) {
		Objects.requireNonNull(applicationId, "applicationId can't be null");
		this.applicationId = applicationId;

		topicStore.setApplicationId(this.applicationId);

		return this;
	}

	public synchronized final InternalTopologyBuilder rewriteTopology(final StreamsConfig config) {
		Objects.requireNonNull(config, "config can't be null");

		// set application id
		setApplicationId(config.getString(StreamsConfig.APPLICATION_ID_CONFIG));

		this.topicStore.rewriteTopology(config);

		return this;
	}

	public final void addSource(final Topology.AutoOffsetReset offsetReset, final String name,
			final TimestampExtractor timestampExtractor, final Deserializer keyDeserializer,
			final Deserializer valDeserializer, final String... topics) {
		topicPatterns.addSource(offsetReset, name, timestampExtractor, keyDeserializer, valDeserializer, topics);
	}

	public final void addSource(final Topology.AutoOffsetReset offsetReset, final String name,
			final TimestampExtractor timestampExtractor, final Deserializer keyDeserializer,
			final Deserializer valDeserializer, final Pattern topicPattern) {
		topicPatterns.addSource(offsetReset, name, timestampExtractor, keyDeserializer, valDeserializer);
	}

	public final <K, V> void addSink(final String name, final String topic, final Serializer<K> keySerializer,
			final Serializer<V> valSerializer, final StreamPartitioner<? super K, ? super V> partitioner,
			final String... predecessorNames) {
		Objects.requireNonNull(name, "name must not be null");
		Objects.requireNonNull(topic, "topic must not be null");
		Objects.requireNonNull(predecessorNames, "predecessor names must not be null");
		if (predecessorNames.length == 0) {
			throw new TopologyException("Sink " + name + " must have at least one parent");
		}

		addSink(name, new StaticTopicNameExtractor<>(topic), keySerializer, valSerializer, partitioner,
				predecessorNames);
		sourceSink.addSinkTopicToNode(name, topic);
		topicStore.setNodeGroups(null);
	}

	public final <K, V> void addSink(final String name, final TopicNameExtractor<K, V> topicExtractor,
			final Serializer<K> keySerializer, final Serializer<V> valSerializer,
			final StreamPartitioner<? super K, ? super V> partitioner, final String... predecessorNames) {
		topicPatterns.addSink(name, topicExtractor, keySerializer, valSerializer, partitioner, predecessorNames);
	}

	public final void addProcessor(final String name, final ProcessorSupplier supplier,
			final String... predecessorNames) {
		topicPatterns.addProcessor(name, supplier, predecessorNames);
	}

	public final void addStateStore(final StoreBuilder<?> storeBuilder, final String... processorNames) {
		addStateStore(storeBuilder, false, processorNames);
	}

	public final void addStateStore(final StoreBuilder<?> storeBuilder, final boolean allowOverride,
			final String... processorNames) {
		Objects.requireNonNull(storeBuilder, "storeBuilder can't be null");
		topicStore.addStateStore(nodeGrouper, nodeFactories, storeBuilder, allowOverride, processorNames);
	}

	public final void addGlobalStore(final StoreBuilder storeBuilder, final String sourceName,
			final TimestampExtractor timestampExtractor, final Deserializer keyDeserializer,
			final Deserializer valueDeserializer, final String topic, final String processorName,
			final ProcessorSupplier stateUpdateSupplier) {
		topicPatterns.addGlobalStore(storeBuilder, sourceName, timestampExtractor, keyDeserializer, valueDeserializer,
				topic, processorName, stateUpdateSupplier);
	}

	public final void connectProcessorAndStateStores(final String processorName, final String... stateStoreNames) {
		topicStore.connectProcessorAndStateStores(processorName, stateStoreNames);
	}

	public void connectSourceStoreAndTopic(final String sourceStoreName, final String topic) {
		topicStore.connectSourceStoreAndTopic(sourceStoreName, topic);
	}

	public final void addInternalTopic(final String topicName) {
		topicStore.addInternalTopic(topicName);
	}

	public final void copartitionSources(final Collection<String> sourceNodes) {
		sourceSink.copartitionSources(sourceNodes);
	}

	public synchronized Map<Integer, Set<String>> nodeGroups() {
		return topicStore.nodeGroups();
	}

	public synchronized ProcessorTopology build() {
		return build((Integer) null);
	}

	public synchronized ProcessorTopology build(final Integer topicGroupId) {
		final Set<String> nodeGroup;
		if (topicGroupId != null) {
			nodeGroup = nodeGroups().get(topicGroupId);
		} else {
			// when topicGroupId is null, we build the full topology minus the global groups
			final Set<String> globalNodeGroups = globalNodeGroups();
			final Collection<Set<String>> values = nodeGroups().values();
			nodeGroup = new HashSet<>();
			for (final Set<String> value : values) {
				nodeGroup.addAll(value);
			}
			nodeGroup.removeAll(globalNodeGroups);
		}
		return topicStore.build(nodeFactories, subscriptionUpdates, nodeGroup);
	}

	/**
	 * Builds the topology for any global state stores
	 * 
	 * @return ProcessorTopology
	 */
	public synchronized ProcessorTopology buildGlobalStateTopology() {
		Objects.requireNonNull(applicationId, "topology has not completed optimization");

		final Set<String> globalGroups = globalNodeGroups();
		if (globalGroups.isEmpty()) {
			return null;
		}
		return topicStore.build(nodeFactories, subscriptionUpdates, globalGroups);
	}

	private Set<String> globalNodeGroups() {
		final Set<String> globalGroups = new HashSet<>();
		for (final Map.Entry<Integer, Set<String>> nodeGroup : nodeGroups().entrySet()) {
			final Set<String> nodes = nodeGroup.getValue();
			for (final String node : nodes) {
				if (isGlobalSource(node)) {
					globalGroups.addAll(nodes);
				}
			}
		}
		return globalGroups;
	}

	/**
	 * Get any global {@link StateStore}s that are part of the topology
	 * 
	 * @return map containing all global {@link StateStore}s
	 */
	public Map<String, StateStore> globalStateStores() {
		Objects.requireNonNull(applicationId, "topology has not completed optimization");

		return topicStore.globalStateStores();
	}

	public Set<String> allStateStoreName() {
		Objects.requireNonNull(applicationId, "topology has not completed optimization");

		return topicStore.allStateStoreName();
	}

	/**
	 * Returns the map of topic groups keyed by the group id. A topic group is a
	 * group of topics in the same task.
	 *
	 * @return groups of topic names
	 */
	public synchronized Map<Integer, TopicsInfo> topicGroups() {
		return topicStore.topicGroups();
	}

	public synchronized Pattern earliestResetTopicsPattern() {
		return topicPatterns.earliestResetTopicsPattern();
	}

	public synchronized Pattern latestResetTopicsPattern() {
		return topicPatterns.latestResetTopicsPattern();
	}

	public Map<String, List<String>> stateStoreNameToSourceTopics() {
		return sourceSink.stateStoreNameToSourceTopics(topicStore);
	}

	public synchronized Collection<Set<String>> copartitionGroups() {
		return sourceSink.copartitionGroups(topicStore);
	}

	SubscriptionUpdates subscriptionUpdates() {
		return subscriptionUpdates;
	}

	synchronized Pattern sourceTopicPattern() {
		if (topicPattern == null) {
			topicPattern = sourceSink.sourceTopicPattern(topicStore);
		}

		return topicPattern;
	}

	// package-private for testing only
	synchronized void updateSubscriptions(final SubscriptionUpdates subscriptionUpdates, final String logPrefix) {
		log.debug("{}updating builder with {} topic(s) with possible matching regex subscription(s)", logPrefix,
				subscriptionUpdates);
		this.subscriptionUpdates = subscriptionUpdates;
		sourceSink.setRegexMatchedTopicsToSourceNodes();
		sourceSink.setRegexMatchedTopicToStateStore();
	}

	private boolean isGlobalSource(final String nodeName) {
		final NodeFactory nodeFactory = nodeFactories.get(nodeName);

		if (nodeFactory instanceof SourceNodeFactory) {
			final List<String> topics = ((SourceNodeFactory) nodeFactory).getTopics();
			return topics != null && topics.size() == 1 && globalTopics.contains(topics.get(0));
		}
		return false;
	}

	public TopologyDescription describe() {
		final TopologyDescription description = new TopologyDescription();

		for (final Map.Entry<Integer, Set<String>> nodeGroup : topicStore.nodeGroups().entrySet()) {

			final Set<String> allNodesOfGroups = nodeGroup.getValue();
			final boolean isNodeGroupOfGlobalStores = nodeGroupContainsGlobalSourceNode(allNodesOfGroups);

			if (!isNodeGroupOfGlobalStores) {
				describeSubtopology(description, nodeGroup.getKey(), allNodesOfGroups);
			} else {
				sourceSink.describeGlobalStore(description, allNodesOfGroups, nodeGroup.getKey());
			}
		}

		return description;
	}

	private boolean nodeGroupContainsGlobalSourceNode(final Set<String> allNodesOfGroups) {
		for (final String node : allNodesOfGroups) {
			if (isGlobalSource(node)) {
				return true;
			}
		}
		return false;
	}

	private static class NodeComparator implements Comparator<TopologyDescription.Node>, Serializable {

		@Override
		public int compare(final TopologyDescription.Node node1, final TopologyDescription.Node node2) {
			final int size1 = ((AbstractNode) node1).size;
			final int size2 = ((AbstractNode) node2).size;

			// it is possible that two nodes have the same sub-tree size (think two nodes
			// connected via state stores)
			// in this case default to processor name string
			if (size1 != size2) {
				return size2 - size1;
			} else {
				return node1.name().compareTo(node2.name());
			}
		}
	}

	private static void updateSize(final AbstractNode node, final int delta) {
		node.size += delta;

		for (final TopologyDescription.Node predecessor : node.predecessors()) {
			updateSize((AbstractNode) predecessor, delta);
		}
	}

	private void describeSubtopology(final TopologyDescription description, final Integer subtopologyId,
			final Set<String> nodeNames) {

		final Map<String, AbstractNode> nodesByName = new HashMap<>();

		// add all nodes
		for (final String nodeName : nodeNames) {
			nodesByName.put(nodeName, nodeFactories.get(nodeName).describe());
		}

		// connect each node to its predecessors and successors
		for (final AbstractNode node : nodesByName.values()) {
			for (final String predecessorName : nodeFactories.get(node.name()).getPredecessors()) {
				final AbstractNode predecessor = nodesByName.get(predecessorName);
				node.addPredecessor(predecessor);
				predecessor.addSuccessor(node);
				updateSize(predecessor, node.size);
			}
		}

		description.addSubtopology(new Subtopology(subtopologyId, new HashSet<>(nodesByName.values())));
	}

	public final static class GlobalStore implements TopologyDescription.GlobalStore {
		private final Source source;
		private final Processor processor;
		private final int id;

		public GlobalStore(final String sourceName, final String processorName, final String storeName,
				final String topicName, final int id) {
			source = new Source(sourceName, Collections.singleton(topicName), null);
			processor = new Processor(processorName, Collections.singleton(storeName));
			source.successors.add(processor);
			processor.predecessors.add(source);
			this.id = id;
		}

		@Override
		public int id() {
			return id;
		}

		@Override
		public TopologyDescription.Source source() {
			return source;
		}

		@Override
		public TopologyDescription.Processor processor() {
			return processor;
		}

		@Override
		public String toString() {
			return "Sub-topology: " + id + " for global store (will not generate tasks)\n" + "    " + source.toString()
					+ "\n" + "    " + processor.toString() + "\n";
		}

		@Override
		public boolean equals(final Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			final GlobalStore that = (GlobalStore) o;
			return source.equals(that.source) && processor.equals(that.processor);
		}

		@Override
		public int hashCode() {
			return Objects.hash(source, processor);
		}
	}

	public abstract static class AbstractNode implements TopologyDescription.Node {
		final String name;
		final Set<TopologyDescription.Node> predecessors = new TreeSet<>(NODE_COMPARATOR);
		final Set<TopologyDescription.Node> successors = new TreeSet<>(NODE_COMPARATOR);

		// size of the sub-topology rooted at this node, including the node itself
		int size;

		AbstractNode(final String name) {
			this.name = name;
			this.size = 1;
		}

		@Override
		public String name() {
			return name;
		}

		@Override
		public Set<TopologyDescription.Node> predecessors() {
			return Collections.unmodifiableSet(predecessors);
		}

		@Override
		public Set<TopologyDescription.Node> successors() {
			return Collections.unmodifiableSet(successors);
		}

		public void addPredecessor(final TopologyDescription.Node predecessor) {
			predecessors.add(predecessor);
		}

		public void addSuccessor(final TopologyDescription.Node successor) {
			successors.add(successor);
		}
	}

	public final static class Source extends AbstractNode implements TopologyDescription.Source {
		private final Set<String> topics;
		private final Pattern topicPattern;

		public Source(final String name, final Set<String> topics, final Pattern pattern) {
			super(name);
			this.topics = topics;
			this.topicPattern = pattern;
		}

		@Deprecated
		@Override
		public String topics() {
			return topics.toString();
		}

		@Override
		public Set<String> topicSet() {
			return topics;
		}

		@Override
		public Pattern topicPattern() {
			return topicPattern;
		}

		@Override
		public void addPredecessor(final TopologyDescription.Node predecessor) {
			throw new UnsupportedOperationException("Sources don't have predecessors.");
		}

		@Override
		public String toString() {
			final String topicsString = topics == null ? topicPattern.toString() : topics.toString();

			return "Source: " + name + " (topics: " + topicsString + ")\n      --> " + nodeNames(successors);
		}

		@Override
		public boolean equals(final Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			final Source source = (Source) o;
			// omit successor to avoid infinite loops
			return name.equals(source.name) && topics.equals(source.topics) && topicPattern.equals(source.topicPattern);
		}

		@Override
		public int hashCode() {
			// omit successor as it might change and alter the hash code
			return Objects.hash(name, topics, topicPattern);
		}
	}

	public final static class Processor extends AbstractNode implements TopologyDescription.Processor {
		private final Set<String> stores;

		public Processor(final String name, final Set<String> stores) {
			super(name);
			this.stores = stores;
		}

		@Override
		public Set<String> stores() {
			return Collections.unmodifiableSet(stores);
		}

		@Override
		public String toString() {
			return "Processor: " + name + " (stores: " + stores + ")\n      --> " + nodeNames(successors)
					+ "\n      <-- " + nodeNames(predecessors);
		}

		@Override
		public boolean equals(final Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			final Processor processor = (Processor) o;
			// omit successor to avoid infinite loops
			return name.equals(processor.name) && stores.equals(processor.stores)
					&& predecessors.equals(processor.predecessors);
		}

		@Override
		public int hashCode() {
			// omit successor as it might change and alter the hash code
			return Objects.hash(name, stores);
		}
	}

	public final static class Sink extends AbstractNode implements TopologyDescription.Sink {
		private final TopicNameExtractor topicNameExtractor;

		public Sink(final String name, final TopicNameExtractor topicNameExtractor) {
			super(name);
			this.topicNameExtractor = topicNameExtractor;
		}

		public Sink(final String name, final String topic) {
			super(name);
			this.topicNameExtractor = new StaticTopicNameExtractor(topic);
		}

		@Override
		public String topic() {
			if (topicNameExtractor instanceof StaticTopicNameExtractor) {
				return ((StaticTopicNameExtractor) topicNameExtractor).topicName;
			} else {
				return null;
			}
		}

		@Override
		public TopicNameExtractor topicNameExtractor() {
			if (topicNameExtractor instanceof StaticTopicNameExtractor) {
				return null;
			} else {
				return topicNameExtractor;
			}
		}

		@Override
		public void addSuccessor(final TopologyDescription.Node successor) {
			throw new UnsupportedOperationException("Sinks don't have successors.");
		}

		@Override
		public String toString() {
			if (topicNameExtractor instanceof StaticTopicNameExtractor) {
				return "Sink: " + name + " (topic: " + topic() + ")\n      <-- " + nodeNames(predecessors);
			}
			return "Sink: " + name + " (extractor class: " + topicNameExtractor + ")\n      <-- "
					+ nodeNames(predecessors);
		}

		@Override
		public boolean equals(final Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			final Sink sink = (Sink) o;
			return name.equals(sink.name) && topicNameExtractor.equals(sink.topicNameExtractor)
					&& predecessors.equals(sink.predecessors);
		}

		@Override
		public int hashCode() {
			// omit predecessors as it might change and alter the hash code
			return Objects.hash(name, topicNameExtractor);
		}
	}

	public final static class Subtopology implements org.apache.kafka.streams.TopologyDescription.Subtopology {
		private final int id;
		private final Set<TopologyDescription.Node> nodes;

		public Subtopology(final int id, final Set<TopologyDescription.Node> nodes) {
			this.id = id;
			this.nodes = new TreeSet<>(NODE_COMPARATOR);
			this.nodes.addAll(nodes);
		}

		@Override
		public int id() {
			return id;
		}

		@Override
		public Set<TopologyDescription.Node> nodes() {
			return Collections.unmodifiableSet(nodes);
		}

		// visible for testing
		Iterator<TopologyDescription.Node> nodesInOrder() {
			return nodes.iterator();
		}

		@Override
		public String toString() {
			return "Sub-topology: " + id + "\n" + nodesAsString() + "\n";
		}

		private String nodesAsString() {
			final StringBuilder sb = new StringBuilder();
			for (final TopologyDescription.Node node : nodes) {
				sb.append("    ");
				sb.append(node);
				sb.append('\n');
			}
			return sb.toString();
		}

		@Override
		public boolean equals(final Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			final Subtopology that = (Subtopology) o;
			return id == that.id && nodes.equals(that.nodes);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, nodes);
		}
	}

	public static class TopicsInfo {
		final Set<String> sinkTopics;
		final Set<String> sourceTopics;
		public final Map<String, InternalTopicConfig> stateChangelogTopics;
		public final Map<String, InternalTopicConfig> repartitionSourceTopics;

		TopicsInfo(final Set<String> sinkTopics, final Set<String> sourceTopics,
				final Map<String, InternalTopicConfig> repartitionSourceTopics,
				final Map<String, InternalTopicConfig> stateChangelogTopics) {
			this.sinkTopics = sinkTopics;
			this.sourceTopics = sourceTopics;
			this.stateChangelogTopics = stateChangelogTopics;
			this.repartitionSourceTopics = repartitionSourceTopics;
		}

		@Override
		public boolean equals(final Object o) {
			if (o instanceof TopicsInfo) {
				final TopicsInfo other = (TopicsInfo) o;
				return other.sourceTopics.equals(sourceTopics)
						&& other.stateChangelogTopics.equals(stateChangelogTopics);
			} else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			final long n = ((long) sourceTopics.hashCode() << 32) | (long) stateChangelogTopics.hashCode();
			return (int) (n % 0xFFFFFFFFL);
		}

		@Override
		public String toString() {
			return "TopicsInfo{" + "sinkTopics=" + sinkTopics + ", sourceTopics=" + sourceTopics
					+ ", repartitionSourceTopics=" + repartitionSourceTopics + ", stateChangelogTopics="
					+ stateChangelogTopics + '}';
		}
	}

	private static class GlobalStoreComparator implements Comparator<TopologyDescription.GlobalStore>, Serializable {
		@Override
		public int compare(final TopologyDescription.GlobalStore globalStore1,
				final TopologyDescription.GlobalStore globalStore2) {
			return globalStore1.id() - globalStore2.id();
		}
	}

	private static class SubtopologyComparator implements Comparator<TopologyDescription.Subtopology>, Serializable {
		@Override
		public int compare(final TopologyDescription.Subtopology subtopology1,
				final TopologyDescription.Subtopology subtopology2) {
			return subtopology1.id() - subtopology2.id();
		}
	}

	public final static class TopologyDescription implements org.apache.kafka.streams.TopologyDescription {
		private final TreeSet<TopologyDescription.Subtopology> subtopologies = new TreeSet<>(SUBTOPOLOGY_COMPARATOR);
		private final TreeSet<TopologyDescription.GlobalStore> globalStores = new TreeSet<>(GLOBALSTORE_COMPARATOR);

		public void addSubtopology(final TopologyDescription.Subtopology subtopology) {
			subtopologies.add(subtopology);
		}

		public void addGlobalStore(final TopologyDescription.GlobalStore globalStore) {
			globalStores.add(globalStore);
		}

		@Override
		public Set<TopologyDescription.Subtopology> subtopologies() {
			return Collections.unmodifiableSet(subtopologies);
		}

		@Override
		public Set<TopologyDescription.GlobalStore> globalStores() {
			return Collections.unmodifiableSet(globalStores);
		}

		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder();
			sb.append("Topologies:\n ");
			final TopologyDescription.Subtopology[] sortedSubtopologies = subtopologies.descendingSet()
					.toArray(new Subtopology[0]);
			final TopologyDescription.GlobalStore[] sortedGlobalStores = globalStores.descendingSet()
					.toArray(new GlobalStore[0]);
			int expectedId = 0;
			int subtopologiesIndex = sortedSubtopologies.length - 1;
			int globalStoresIndex = sortedGlobalStores.length - 1;
			while (subtopologiesIndex != -1 && globalStoresIndex != -1) {
				sb.append("  ");
				final TopologyDescription.Subtopology subtopology = sortedSubtopologies[subtopologiesIndex];
				final TopologyDescription.GlobalStore globalStore = sortedGlobalStores[globalStoresIndex];
				if (subtopology.id() == expectedId) {
					sb.append(subtopology);
					subtopologiesIndex--;
				} else {
					sb.append(globalStore);
					globalStoresIndex--;
				}
				expectedId++;
			}
			while (subtopologiesIndex != -1) {
				final TopologyDescription.Subtopology subtopology = sortedSubtopologies[subtopologiesIndex];
				sb.append("  ");
				sb.append(subtopology);
				subtopologiesIndex--;
			}
			while (globalStoresIndex != -1) {
				final TopologyDescription.GlobalStore globalStore = sortedGlobalStores[globalStoresIndex];
				sb.append("  ");
				sb.append(globalStore);
				globalStoresIndex--;
			}
			return sb.toString();
		}

		@Override
		public boolean equals(final Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			final TopologyDescription that = (TopologyDescription) o;
			return subtopologies.equals(that.subtopologies) && globalStores.equals(that.globalStores);
		}

		@Override
		public int hashCode() {
			return Objects.hash(subtopologies, globalStores);
		}

	}

	private static String nodeNames(final Set<TopologyDescription.Node> nodes) {
		final StringBuilder sb = new StringBuilder();
		if (!nodes.isEmpty()) {
			for (final TopologyDescription.Node n : nodes) {
				sb.append(n.name());
				sb.append(", ");
			}
			sb.deleteCharAt(sb.length() - 1);
			sb.deleteCharAt(sb.length() - 1);
		} else {
			return "none";
		}
		return sb.toString();
	}

	/**
	 * Used to capture subscribed topic via Patterns discovered during the partition
	 * assignment process.
	 */
	public static class SubscriptionUpdates {

		private final Set<String> updatedTopicSubscriptions = new HashSet<>();

		private void updateTopics(final Collection<String> topicNames) {
			updatedTopicSubscriptions.clear();
			updatedTopicSubscriptions.addAll(topicNames);
		}

		public Collection<String> getUpdates() {
			return Collections.unmodifiableSet(updatedTopicSubscriptions);
		}

		boolean hasUpdates() {
			return !updatedTopicSubscriptions.isEmpty();
		}

		@Override
		public String toString() {
			return String.format("SubscriptionUpdates{updatedTopicSubscriptions=%s}", updatedTopicSubscriptions);
		}
	}

	void updateSubscribedTopics(final Set<String> topics, final String logPrefix) {
		final SubscriptionUpdates subscriptionUpdates = new SubscriptionUpdates();
		log.debug("{}found {} topics possibly matching regex", logPrefix, topics);
		// update the topic groups with the returned subscription set for regex pattern
		// subscriptions
		subscriptionUpdates.updateTopics(topics);
		updateSubscriptions(subscriptionUpdates, logPrefix);
	}

	// following functions are for test only

	public synchronized Set<String> getSourceTopicNames() {
		return topicPatterns.getSourceTopicNames();
	}

	public synchronized Map<String, StateStoreFactory> getStateStores() {
		return topicStore.getStateFactories();
	}
}
