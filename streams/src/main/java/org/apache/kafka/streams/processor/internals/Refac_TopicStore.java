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
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.TopicsInfo;
import org.apache.kafka.streams.processor.internals.nf.NodeFactory;
import org.apache.kafka.streams.processor.internals.nf.ProcessorNodeFactory;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.SessionStoreBuilder;
import org.apache.kafka.streams.state.internals.TimestampedWindowStoreBuilder;
import org.apache.kafka.streams.state.internals.WindowStoreBuilder;

public class Refac_TopicStore implements ITopicStore {
	static class StateStoreFactory {
		private final StoreBuilder builder;
		private final Set<String> users = new HashSet<>();

		private StateStoreFactory(final StoreBuilder<?> builder) {
			this.builder = builder;
		}

		public StateStore build() {
			return builder.build();
		}

		public boolean isWindowStore() {
			return builder instanceof WindowStoreBuilder || builder instanceof TimestampedWindowStoreBuilder
					|| builder instanceof SessionStoreBuilder;
		}

		// Apparently Java strips the generics from this method because we're using the
		// raw type for builder,
		// even though this method doesn't use builder's (missing) type parameter. Our
		// usage seems obviously
		// correct, though, hence the suppression.
		@SuppressWarnings("unchecked")
		public Map<String, String> logConfig() {
			return builder.logConfig();
		}

		public boolean loggingEnabled() {
			return builder.loggingEnabled();
		}

		private String name() {
			return builder.name();
		}

		private Set<String> users() {
			return users;
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
	}

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
	private final Refac_SourceSink sourceSink;
	private final Refac_GlobalTopics globalTopics;
	private String applicationId;
	private final QuickUnion<String> nodeGrouper;
	private final Map<String, NodeFactory> nodeFactories;

	private final Refac_NodeBuilder nodeBuilder;

	public Refac_TopicStore(Refac_SourceSink sourceSink, Refac_GlobalTopics globalTopics,
			QuickUnion<String> nodeGrouper, Map<String, NodeFactory> nodeFactories) {
		this.sourceSink = sourceSink;
		this.globalTopics = globalTopics;
		this.nodeGrouper = nodeGrouper;
		this.nodeFactories = nodeFactories;
		this.nodeBuilder = new Refac_NodeBuilder(globalStateStores, storeToChangelogTopic, stateFactories,
				internalTopicNames);
	}

	public final void addInternalTopic(final String topicName) {
		Objects.requireNonNull(topicName, "topicName can't be null");
		internalTopicNames.add(topicName);
	}

	@Override
	public void addPatternForTopic(String update, Pattern pattern) {
		topicToPatterns.put(update, pattern);
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

	@Override
	public void addToGlobalStateBuilder(StoreBuilder storeBuilder) {
		globalStateBuilders.put(storeBuilder.name(), storeBuilder);
	}

	public Set<String> allStateStoreName() {
		final Set<String> allNames = new HashSet<>(stateFactories.keySet());
		allNames.addAll(globalStateStores.keySet());
		return Collections.unmodifiableSet(allNames);
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

	@Override
	public void connectSourceStoreAndTopic(final String sourceStoreName, final String topic) {
		if (storeToChangelogTopic.containsKey(sourceStoreName)) {
			throw new TopologyException("Source store " + sourceStoreName + " is already added.");
		}
		storeToChangelogTopic.put(sourceStoreName, topic);
	}

	@Override
	public boolean containsTopic(String topicName) {
		return internalTopicNames.contains(topicName);
	}

	@Override
	public List<String> decorateInternalSourceTopics(final Collection<String> sourceTopics) {
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

	@Override
	public String decorateTopic(final String topic) {
		return nodeBuilder.decorateTopic(topic);
	}

	public Map<String, StateStoreFactory> getStateFactories() {
		return stateFactories;
	}

	public Map<String, StateStore> globalStateStores() {
		return Collections.unmodifiableMap(globalStateStores);
	}

	@Override
	public boolean hasPatternForTopic(String topic) {
		return topicToPatterns.containsKey(topic);
	}

	@Override
	public Map<Integer, Set<String>> nodeGroups() {
		if (nodeGroups == null) {
			nodeGroups = sourceSink.makeNodeGroups();
		}
		return nodeGroups;
	}

	public final void rewriteTopology(final StreamsConfig config) {
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

	public void setApplicationId(String applicationId) {
		this.applicationId = applicationId;
		this.nodeBuilder.setApplicationId(applicationId);
	}

	@Override
	public void setNodeGroups(Map<Integer, Set<String>> nodeGroups) {
		this.nodeGroups = nodeGroups;
	}

	@Override
	public Pattern topicForPattern(String topic) {
		return topicToPatterns.get(topic);
	}

	public Map<Integer, TopicsInfo> topicGroups() {
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
							final InternalTopicConfig internalTopicConfig = Refac_TopicHelper
									.createChangelogTopicConfig(stateFactory, topicName);
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

	@Override
	public boolean validateStoreName(String storeName) {
		return !stateFactories.containsKey(storeName) && !globalStateBuilders.containsKey(storeName);
	}
}
