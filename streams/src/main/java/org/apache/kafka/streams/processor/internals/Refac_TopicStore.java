package org.apache.kafka.streams.processor.internals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.nf.NodeFactory;
import org.apache.kafka.streams.processor.internals.nf.ProcessorNodeFactory;
import org.apache.kafka.streams.processor.internals.nf.SinkNodeFactory;
import org.apache.kafka.streams.processor.internals.nf.SourceNodeFactory;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.SubscriptionUpdates;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.TopicsInfo;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.SessionStoreBuilder;
import org.apache.kafka.streams.state.internals.TimestampedWindowStoreBuilder;
import org.apache.kafka.streams.state.internals.WindowStoreBuilder;

public class Refac_TopicStore {
	private static final Pattern EMPTY_ZERO_LENGTH_PATTERN = Pattern.compile("");
	private final Map<String, StateStoreFactory> stateFactories = new HashMap<>();

	private final Set<String> internalTopicNames = new HashSet<>();

	private final Map<String, StateStore> globalStateStores = new LinkedHashMap<>();

	private final Map<String, StoreBuilder> globalStateBuilders = new LinkedHashMap<>();
	private Map<Integer, Set<String>> nodeGroups = null;
	private final Map<String, String> storeToChangelogTopic = new HashMap<>();

	// map from topics to their matched regex patterns, this is to ensure one topic
	// is passed through on source node
	// even if it can be matched by multiple regex patterns
	private final Map<String, Pattern> topicToPatterns = new HashMap<>();
	private Refac_SourceSink sourceSink;
	private Refac_GlobalTopics globalTopics;
	private String applicationId;
	private QuickUnion<String> nodeGrouper;
	private Map<String, NodeFactory> nodeFactories;

	public Refac_TopicStore(Refac_SourceSink sourceSink, Refac_GlobalTopics globalTopics, QuickUnion<String> nodeGrouper, Map<String, NodeFactory> nodeFactories) {
		this.sourceSink = sourceSink;
		this.globalTopics = globalTopics;
		this.nodeGrouper = nodeGrouper;
		this.nodeFactories = nodeFactories;
	}

	public boolean containsTopic(String topicName) {
		return internalTopicNames.contains(topicName);
	}

	public void addToGlobalStateBuilder(StoreBuilder storeBuilder) {
		globalStateBuilders.put(storeBuilder.name(), storeBuilder);
	}

	public Map<String, StateStore> globalStateStores() {
		return Collections.unmodifiableMap(globalStateStores);
	}

	public Map<Integer, Set<String>> nodeGroups() {
		if (nodeGroups == null) {
			nodeGroups = sourceSink.makeNodeGroups();
		}
		return nodeGroups;
	}

	public Map<String, StateStoreFactory> getStateFactories() {
		return stateFactories;
	}

	public Set<String> allStateStoreName() {
		final Set<String> allNames = new HashSet<>(stateFactories.keySet());
		allNames.addAll(globalStateStores.keySet());
		return Collections.unmodifiableSet(allNames);
	}

	public void setNodeGroups(Map<Integer, Set<String>> nodeGroups) {
		this.nodeGroups = nodeGroups;
	}

	public final void addStateStore(QuickUnion<String> nodeGrouper, Map<String, NodeFactory> nodeFactories,
			final StoreBuilder<?> storeBuilder, final boolean allowOverride, final String... processorNames) {
		if (!allowOverride && stateFactories.containsKey(storeBuilder.name())) {
			throw new TopologyException("StateStore " + storeBuilder.name() + " is already added.");
		}

		stateFactories.put(storeBuilder.name(), new StateStoreFactory(storeBuilder));

		if (processorNames != null) {
			for (final String processorName : processorNames) {
				Objects.requireNonNull(processorName, "processor name must not be null");
				connectProcessorAndStateStore(nodeGrouper, nodeFactories, processorName, storeBuilder.name());
			}
		}
		setNodeGroups(null);
	}

	public synchronized final void rewriteTopology(final StreamsConfig config) {
		// maybe strip out caching layers
		if (config.getLong(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG) == 0L) {
			for (final StateStoreFactory storeFactory : stateFactories.values()) {
				storeFactory.builder.withCachingDisabled();
			}

			for (final StoreBuilder storeBuilder : globalStateBuilders.values()) {
				storeBuilder.withCachingDisabled();
			}
		}

		// build global state stores
		for (final StoreBuilder storeBuilder : globalStateBuilders.values()) {
			globalStateStores.put(storeBuilder.name(), storeBuilder.build());
		}
	}

	public final void addInternalTopic(final String topicName) {
		Objects.requireNonNull(topicName, "topicName can't be null");
		internalTopicNames.add(topicName);
	}

	public synchronized Map<Integer, TopicsInfo> topicGroups() {
		final Map<Integer, TopicsInfo> topicGroups = new LinkedHashMap<>();

		if (nodeGroups == null) {
			nodeGroups = sourceSink.makeNodeGroups();
		}

		for (final Map.Entry<Integer, Set<String>> entry : nodeGroups.entrySet()) {
			final Set<String> sinkTopics = new HashSet<>();
			final Set<String> sourceTopics = new HashSet<>();
			final Map<String, InternalTopicConfig> repartitionTopics = new HashMap<>();
			final Map<String, InternalTopicConfig> stateChangelogTopics = new HashMap<>();
			for (final String node : entry.getValue()) {
				// if the node is a source node, add to the source topics
				final List<String> topics = sourceSink.sourceTopicsForNode(node);
				if (topics != null) {
					// if some of the topics are internal, add them to the internal topics
					for (final String topic : topics) {
						// skip global topic as they don't need partition assignment
						if (globalTopics.contains(topic)) {
							continue;
						}
						if (internalTopicNames.contains(topic)) {
							// prefix the internal topic name with the application id
							final String internalTopic = decorateTopic(topic);
							repartitionTopics.put(internalTopic,
									new RepartitionTopicConfig(internalTopic, Collections.emptyMap()));
							sourceTopics.add(internalTopic);
						} else {
							sourceTopics.add(topic);
						}
					}
				}

				// if the node is a sink node, add to the sink topics
				final String topic = sourceSink.sinkTopicForNode(node);
				if (topic != null) {
					if (internalTopicNames.contains(topic)) {
						// prefix the change log topic name with the application id
						sinkTopics.add(decorateTopic(topic));
					} else {
						sinkTopics.add(topic);
					}
				}

				// if the node is connected to a state store whose changelog topics are not
				// predefined,
				// add to the changelog topics
				for (final StateStoreFactory stateFactory : stateFactories.values()) {
					if (stateFactory.loggingEnabled() && stateFactory.users().contains(node)) {
						final String topicName = storeToChangelogTopic.containsKey(stateFactory.name())
								? storeToChangelogTopic.get(stateFactory.name())
								: ProcessorStateManager.storeChangelogTopic(applicationId, stateFactory.name());
						if (!stateChangelogTopics.containsKey(topicName)) {
							final InternalTopicConfig internalTopicConfig = createChangelogTopicConfig(stateFactory,
									topicName);
							stateChangelogTopics.put(topicName, internalTopicConfig);
						}
					}
				}
			}
			if (!sourceTopics.isEmpty()) {
				topicGroups.put(entry.getKey(), new TopicsInfo(Collections.unmodifiableSet(sinkTopics),
						Collections.unmodifiableSet(sourceTopics), Collections.unmodifiableMap(repartitionTopics),
						Collections.unmodifiableMap(stateChangelogTopics)));
			}
		}

		return Collections.unmodifiableMap(topicGroups);
	}

	public ProcessorTopology build(Map<String, NodeFactory> nodeFactories, SubscriptionUpdates subscriptionUpdates,
			final Set<String> nodeGroup) {
		final Map<String, ProcessorNode> processorMap = new LinkedHashMap<>();
		final Map<String, SourceNode> topicSourceMap = new HashMap<>();
		final Map<String, SinkNode> topicSinkMap = new HashMap<>();
		final Map<String, StateStore> stateStoreMap = new LinkedHashMap<>();
		final Set<String> repartitionTopics = new HashSet<>();

		// create processor nodes in a topological order ("nodeFactories" is already
		// topologically sorted)
		// also make sure the state store map values following the insertion ordering
		for (final NodeFactory factory : nodeFactories.values()) {
			if (nodeGroup == null || nodeGroup.contains(factory.getName())) {
				final ProcessorNode node = factory.build();
				processorMap.put(node.name(), node);

				if (factory instanceof ProcessorNodeFactory) {
					buildProcessorNode(processorMap, stateStoreMap, (ProcessorNodeFactory) factory, node);

				} else if (factory instanceof SourceNodeFactory) {
					buildSourceNode(subscriptionUpdates, topicSourceMap, repartitionTopics, (SourceNodeFactory) factory,
							(SourceNode) node);

				} else if (factory instanceof SinkNodeFactory) {
					buildSinkNode(processorMap, topicSinkMap, repartitionTopics, (SinkNodeFactory) factory,
							(SinkNode) node);
				} else {
					throw new TopologyException("Unknown definition class: " + factory.getClass().getName());
				}
			}
		}

		return new ProcessorTopology(new ArrayList<>(processorMap.values()), topicSourceMap, topicSinkMap,
				new ArrayList<>(stateStoreMap.values()), new ArrayList<>(globalStateStores.values()),
				storeToChangelogTopic, repartitionTopics);
	}

	private void buildProcessorNode(final Map<String, ProcessorNode> processorMap,
			final Map<String, StateStore> stateStoreMap, final ProcessorNodeFactory factory, final ProcessorNode node) {

		for (final String predecessor : factory.getPredecessors()) {
			final ProcessorNode<?, ?> predecessorNode = processorMap.get(predecessor);
			predecessorNode.addChild(node);
		}
		for (final String stateStoreName : factory.getStateStoreNames()) {
			if (!stateStoreMap.containsKey(stateStoreName)) {
				if (stateFactories.containsKey(stateStoreName)) {
					final StateStoreFactory stateStoreFactory = stateFactories.get(stateStoreName);

// remember the changelog topic if this state store is change-logging enabled
					if (stateStoreFactory.loggingEnabled() && !storeToChangelogTopic.containsKey(stateStoreName)) {
						final String changelogTopic = ProcessorStateManager.storeChangelogTopic(applicationId,
								stateStoreName);
						storeToChangelogTopic.put(stateStoreName, changelogTopic);
					}
					stateStoreMap.put(stateStoreName, stateStoreFactory.build());
				} else {
					stateStoreMap.put(stateStoreName, globalStateStores.get(stateStoreName));
				}
			}
		}
	}

	private void buildSourceNode(SubscriptionUpdates subscriptionUpdates, final Map<String, SourceNode> topicSourceMap,
			final Set<String> repartitionTopics, final SourceNodeFactory sourceNodeFactory, final SourceNode node) {

		final List<String> topics = (sourceNodeFactory.getPattern() != null)
				? sourceNodeFactory.getTopics(subscriptionUpdates.getUpdates())
				: sourceNodeFactory.getTopics();

		for (final String topic : topics) {
			if (internalTopicNames.contains(topic)) {
// prefix the internal topic name with the application id
				final String decoratedTopic = decorateTopic(topic);
				topicSourceMap.put(decoratedTopic, node);
				repartitionTopics.add(decoratedTopic);
			} else {
				topicSourceMap.put(topic, node);
			}
		}
	}

	private void buildSinkNode(final Map<String, ProcessorNode> processorMap, final Map<String, SinkNode> topicSinkMap,
			final Set<String> repartitionTopics, final SinkNodeFactory sinkNodeFactory, final SinkNode node) {

		for (final String predecessor : sinkNodeFactory.getPredecessors()) {
			processorMap.get(predecessor).addChild(node);
			if (sinkNodeFactory.getTopicExtractor() instanceof StaticTopicNameExtractor) {
				final String topic = ((StaticTopicNameExtractor) sinkNodeFactory.getTopicExtractor()).topicName;

				if (internalTopicNames.contains(topic)) {
					// prefix the internal topic name with the application id
					final String decoratedTopic = decorateTopic(topic);
					topicSinkMap.put(decoratedTopic, node);
					repartitionTopics.add(decoratedTopic);
				} else {
					topicSinkMap.put(topic, node);
				}

			}
		}
	}

	public List<String> maybeDecorateInternalSourceTopics(final Collection<String> sourceTopics) {
		final List<String> decoratedTopics = new ArrayList<>();
		for (final String topic : sourceTopics) {
			if (internalTopicNames.contains(topic)) {
				decoratedTopics.add(decorateTopic(topic));
			} else {
				decoratedTopics.add(topic);
			}
		}
		return decoratedTopics;
	}

	public String decorateTopic(final String topic) {
		if (applicationId == null) {
			throw new TopologyException("there are internal topics and " + "applicationId hasn't been set. Call "
					+ "setApplicationId first");
		}

		return applicationId + "-" + topic;
	}
	
	public final void connectProcessorAndStateStores(final String processorName, final String... stateStoreNames) {
		Objects.requireNonNull(processorName, "processorName can't be null");
		Objects.requireNonNull(stateStoreNames, "state store list must not be null");
		if (stateStoreNames.length == 0) {
			throw new TopologyException("Must provide at least one state store name.");
		}
		for (final String stateStoreName : stateStoreNames) {
			Objects.requireNonNull(stateStoreName, "state store name must not be null");
			connectProcessorAndStateStore(nodeGrouper, nodeFactories, processorName, stateStoreName);
		}
		setNodeGroups(null);
	}

	public void connectSourceStoreAndTopic(final String sourceStoreName, final String topic) {
		if (storeToChangelogTopic.containsKey(sourceStoreName)) {
			throw new TopologyException("Source store " + sourceStoreName + " is already added.");
		}
		storeToChangelogTopic.put(sourceStoreName, topic);
	}

	public void connectProcessorAndStateStore(QuickUnion<String> nodeGrouper, Map<String, NodeFactory> nodeFactories,
			final String processorName, final String stateStoreName) {
		if (globalStateBuilders.containsKey(stateStoreName)) {
			throw new TopologyException("Global StateStore " + stateStoreName
					+ " can be used by a Processor without being specified; it should not be explicitly passed.");
		}
		if (!stateFactories.containsKey(stateStoreName)) {
			throw new TopologyException("StateStore " + stateStoreName + " is not added yet.");
		}
		if (!nodeFactories.containsKey(processorName)) {
			throw new TopologyException("Processor " + processorName + " is not added yet.");
		}

		final StateStoreFactory stateStoreFactory = stateFactories.get(stateStoreName);
		final Iterator<String> iter = stateStoreFactory.users().iterator();
		if (iter.hasNext()) {
			final String user = iter.next();
			nodeGrouper.unite(user, processorName);
		}
		stateStoreFactory.users().add(processorName);

		final NodeFactory nodeFactory = nodeFactories.get(processorName);
		if (nodeFactory instanceof ProcessorNodeFactory) {
			final ProcessorNodeFactory processorNodeFactory = (ProcessorNodeFactory) nodeFactory;
			processorNodeFactory.addStateStore(stateStoreName);
			sourceSink.connectStateStoreNameToSourceTopicsOrPattern(nodeFactories, stateStoreName,
					processorNodeFactory);
		} else {
			throw new TopologyException(
					"cannot connect a state store " + stateStoreName + " to a source node or a sink node.");
		}
	}

	private InternalTopicConfig createChangelogTopicConfig(final StateStoreFactory factory, final String name) {
		if (factory.isWindowStore()) {
			final WindowedChangelogTopicConfig config = new WindowedChangelogTopicConfig(name, factory.logConfig());
			config.setRetentionMs(factory.retentionPeriod());
			return config;
		} else {
			return new UnwindowedChangelogTopicConfig(name, factory.logConfig());
		}
	}

	static class StateStoreFactory {
		private final StoreBuilder builder;
		private final Set<String> users = new HashSet<>();

		private StateStoreFactory(final StoreBuilder<?> builder) {
			this.builder = builder;
		}

		public StateStore build() {
			return builder.build();
		}

		long retentionPeriod() {
			if (builder instanceof WindowStoreBuilder) {
				return ((WindowStoreBuilder) builder).retentionPeriod();
			} else if (builder instanceof TimestampedWindowStoreBuilder) {
				return ((TimestampedWindowStoreBuilder) builder).retentionPeriod();
			} else if (builder instanceof SessionStoreBuilder) {
				return ((SessionStoreBuilder) builder).retentionPeriod();
			} else {
				throw new IllegalStateException("retentionPeriod is not supported when not a window store");
			}
		}

		private Set<String> users() {
			return users;
		}

		public boolean loggingEnabled() {
			return builder.loggingEnabled();
		}

		private String name() {
			return builder.name();
		}

		private boolean isWindowStore() {
			return builder instanceof WindowStoreBuilder || builder instanceof TimestampedWindowStoreBuilder
					|| builder instanceof SessionStoreBuilder;
		}

		// Apparently Java strips the generics from this method because we're using the
		// raw type for builder,
		// even though this method doesn't use builder's (missing) type parameter. Our
		// usage seems obviously
		// correct, though, hence the suppression.
		@SuppressWarnings("unchecked")
		private Map<String, String> logConfig() {
			return builder.logConfig();
		}
	}

	public boolean validateStoreName(String storeName) {
		return !stateFactories.containsKey(storeName) && !globalStateBuilders.containsKey(storeName);
	}

	public Pattern topicForPattern(String topic) {
		return topicToPatterns.get(topic);
	}

	public boolean hasPatternForTopic(String topic) {
		return topicToPatterns.containsKey(topic);
	}

	public void addPatternForTopic(String update, Pattern pattern) {
		topicToPatterns.put(update, pattern);
	}

	public static Pattern buildPatternForOffsetResetTopics(final Collection<String> sourceTopics,
			final Collection<Pattern> sourcePatterns) {
		final StringBuilder builder = new StringBuilder();

		for (final String topic : sourceTopics) {
			builder.append(topic).append("|");
		}

		for (final Pattern sourcePattern : sourcePatterns) {
			builder.append(sourcePattern.pattern()).append("|");
		}

		if (builder.length() > 0) {
			builder.setLength(builder.length() - 1);
			return Pattern.compile(builder.toString());
		}

		return EMPTY_ZERO_LENGTH_PATTERN;
	}

	public void setApplicationId(String applicationId) {
		this.applicationId = applicationId;
	}
}
