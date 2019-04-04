package org.apache.kafka.streams.processor.internals;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.internals.nf.NodeFactory;
import org.apache.kafka.streams.processor.internals.nf.ProcessorNodeFactory;
import org.apache.kafka.streams.processor.internals.nf.SinkNodeFactory;
import org.apache.kafka.streams.processor.internals.nf.SourceNodeFactory;
import org.apache.kafka.streams.state.StoreBuilder;

public class Refac_TopicPatterns {
	// all topics subscribed from source processors (without application-id prefix
	// for internal topics)
	private final Set<String> sourceTopicNames = new HashSet<>();

	private final Set<String> earliestResetTopics = new HashSet<>();

	private final Set<String> latestResetTopics = new HashSet<>();

	private final Set<Pattern> earliestResetPatterns = new HashSet<>();

	private final Set<Pattern> latestResetPatterns = new HashSet<>();

	private Map<String, NodeFactory> nodeFactories;

	private Refac_SourceSink sourceSink;

	private QuickUnion<String> nodeGrouper;

	private Refac_TopicStore topicStore;

	private Refac_GlobalTopics globalTopics;

	public Refac_TopicPatterns(Map<String, NodeFactory> nodeFactories, Refac_SourceSink sourceSink,
			QuickUnion<String> nodeGrouper, Refac_TopicStore topicStore, Refac_GlobalTopics globalTopics) {
		this.nodeFactories = nodeFactories;
		this.sourceSink = sourceSink;
		this.nodeGrouper = nodeGrouper;
		this.topicStore = topicStore;
		this.globalTopics = globalTopics;
	}

	public final void addSource(final Topology.AutoOffsetReset offsetReset, final String name,
			final TimestampExtractor timestampExtractor, final Deserializer keyDeserializer,
			final Deserializer valDeserializer, final String... topics) {
		if (topics.length == 0) {
			throw new TopologyException("You must provide at least one topic");
		}
		Objects.requireNonNull(name, "name must not be null");
		if (nodeFactories.containsKey(name)) {
			throw new TopologyException("Processor " + name + " is already added.");
		}

		for (final String topic : topics) {
			Objects.requireNonNull(topic, "topic names cannot be null");
			validateTopicNotAlreadyRegistered(topic);
			maybeAddToResetList(earliestResetTopics, latestResetTopics, offsetReset, topic);
			sourceTopicNames.add(topic);
		}

		nodeFactories.put(name, new SourceNodeFactory(name, topics, null, timestampExtractor, keyDeserializer,
				valDeserializer, topicStore, sourceSink));
		sourceSink.addSourceTopicsToNode(name, Arrays.asList(topics));
		nodeGrouper.add(name);
		topicStore.setNodeGroups(null);
	}
	
	public Set<String> getSourceTopicNames() {
		return sourceTopicNames;
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
		Objects.requireNonNull(name, "name must not be null");
		Objects.requireNonNull(topicExtractor, "topic extractor must not be null");
		Objects.requireNonNull(predecessorNames, "predecessor names must not be null");
		if (nodeFactories.containsKey(name)) {
			throw new TopologyException("Processor " + name + " is already added.");
		}
		if (predecessorNames.length == 0) {
			throw new TopologyException("Sink " + name + " must have at least one parent");
		}

		for (final String predecessor : predecessorNames) {
			Objects.requireNonNull(predecessor, "predecessor name can't be null");
			if (predecessor.equals(name)) {
				throw new TopologyException("Processor " + name + " cannot be a predecessor of itself.");
			}
			if (!nodeFactories.containsKey(predecessor)) {
				throw new TopologyException("Predecessor processor " + predecessor + " is not added yet.");
			}
			if (sourceSink.hasSinkTopicForNode(predecessor)) {
				throw new TopologyException("Sink " + predecessor + " cannot be used a parent.");
			}
		}

		nodeFactories.put(name, new SinkNodeFactory<>(name, predecessorNames, topicExtractor, keySerializer,
				valSerializer, partitioner, topicStore));
		nodeGrouper.add(name);
		nodeGrouper.unite(name, predecessorNames);
		topicStore.setNodeGroups(null);
	}

	public final void addSource(final Topology.AutoOffsetReset offsetReset, final String name,
			final TimestampExtractor timestampExtractor, final Deserializer keyDeserializer,
			final Deserializer valDeserializer, final Pattern topicPattern) {
		Objects.requireNonNull(topicPattern, "topicPattern can't be null");
		Objects.requireNonNull(name, "name can't be null");

		if (nodeFactories.containsKey(name)) {
			throw new TopologyException("Processor " + name + " is already added.");
		}

		for (final String sourceTopicName : sourceTopicNames) {
			if (topicPattern.matcher(sourceTopicName).matches()) {
				throw new TopologyException("Pattern " + topicPattern
						+ " will match a topic that has already been registered by another source.");
			}
		}

		for (final Pattern otherPattern : earliestResetPatterns) {
			if (topicPattern.pattern().contains(otherPattern.pattern())
					|| otherPattern.pattern().contains(topicPattern.pattern())) {
				throw new TopologyException("Pattern " + topicPattern + " will overlap with another pattern "
						+ otherPattern + " already been registered by another source");
			}
		}

		for (final Pattern otherPattern : latestResetPatterns) {
			if (topicPattern.pattern().contains(otherPattern.pattern())
					|| otherPattern.pattern().contains(topicPattern.pattern())) {
				throw new TopologyException("Pattern " + topicPattern + " will overlap with another pattern "
						+ otherPattern + " already been registered by another source");
			}
		}

		maybeAddToResetList(earliestResetPatterns, latestResetPatterns, offsetReset, topicPattern);

		nodeFactories.put(name, new SourceNodeFactory(name, null, topicPattern, timestampExtractor, keyDeserializer,
				valDeserializer, topicStore, sourceSink));
		sourceSink.addSourcePatternToNode(name, topicPattern);
		nodeGrouper.add(name);
		topicStore.setNodeGroups(null);
	}

	public Pattern earliestResetTopicsPattern() {
		return resetTopicsPattern(earliestResetTopics, earliestResetPatterns);
	}
	
	public final void addProcessor(final String name, final ProcessorSupplier supplier,
			final String... predecessorNames) {
		Objects.requireNonNull(name, "name must not be null");
		Objects.requireNonNull(supplier, "supplier must not be null");
		Objects.requireNonNull(predecessorNames, "predecessor names must not be null");
		if (nodeFactories.containsKey(name)) {
			throw new TopologyException("Processor " + name + " is already added.");
		}
		if (predecessorNames.length == 0) {
			throw new TopologyException("Processor " + name + " must have at least one parent");
		}

		for (final String predecessor : predecessorNames) {
			Objects.requireNonNull(predecessor, "predecessor name must not be null");
			if (predecessor.equals(name)) {
				throw new TopologyException("Processor " + name + " cannot be a predecessor of itself.");
			}
			if (!nodeFactories.containsKey(predecessor)) {
				throw new TopologyException("Predecessor processor " + predecessor + " is not added yet for " + name);
			}
		}

		nodeFactories.put(name, new ProcessorNodeFactory(name, predecessorNames, supplier));
		nodeGrouper.add(name);
		nodeGrouper.unite(name, predecessorNames);
		topicStore.setNodeGroups(null);
	}
	
	public final void addGlobalStore(final StoreBuilder storeBuilder, final String sourceName,
			final TimestampExtractor timestampExtractor, final Deserializer keyDeserializer,
			final Deserializer valueDeserializer, final String topic, final String processorName,
			final ProcessorSupplier stateUpdateSupplier) {
		Objects.requireNonNull(storeBuilder, "store builder must not be null");
		validateGlobalStoreArguments(sourceName, topic, processorName, stateUpdateSupplier, storeBuilder.name(),
				storeBuilder.loggingEnabled());
		validateTopicNotAlreadyRegistered(topic);

		final String[] topics = { topic };
		final String[] predecessors = { sourceName };

		final ProcessorNodeFactory nodeFactory = new ProcessorNodeFactory(processorName, predecessors,
				stateUpdateSupplier);

		globalTopics.add(topic);
		nodeFactories.put(sourceName, new SourceNodeFactory(sourceName, topics, null, timestampExtractor,
				keyDeserializer, valueDeserializer, topicStore, sourceSink));
		sourceSink.addSourceTopicsToNode(sourceName, Arrays.asList(topics));
		nodeGrouper.add(sourceName);
		nodeFactory.addStateStore(storeBuilder.name());
		nodeFactories.put(processorName, nodeFactory);
		nodeGrouper.add(processorName);
		nodeGrouper.unite(processorName, predecessors);
		topicStore.addToGlobalStateBuilder(storeBuilder);
		topicStore.connectSourceStoreAndTopic(storeBuilder.name(), topic);
		topicStore.setNodeGroups(null);
	}
	
	private void validateGlobalStoreArguments(final String sourceName, final String topic, final String processorName,
			final ProcessorSupplier stateUpdateSupplier, final String storeName, final boolean loggingEnabled) {
		Objects.requireNonNull(sourceName, "sourceName must not be null");
		Objects.requireNonNull(topic, "topic must not be null");
		Objects.requireNonNull(stateUpdateSupplier, "supplier must not be null");
		Objects.requireNonNull(processorName, "processorName must not be null");
		if (nodeFactories.containsKey(sourceName)) {
			throw new TopologyException("Processor " + sourceName + " is already added.");
		}
		if (nodeFactories.containsKey(processorName)) {
			throw new TopologyException("Processor " + processorName + " is already added.");
		}

		if (!topicStore.validateStoreName(storeName)) {
			throw new TopologyException("StateStore " + storeName + " is already added.");
		}
		if (loggingEnabled) {
			throw new TopologyException("StateStore " + storeName + " for global table must not have logging enabled.");
		}
		if (sourceName.equals(processorName)) {
			throw new TopologyException("sourceName and processorName must be different.");
		}
	}
	
	public Pattern latestResetTopicsPattern() {
		return resetTopicsPattern(latestResetTopics, latestResetPatterns);
	}

	private Pattern resetTopicsPattern(final Set<String> resetTopics, final Set<Pattern> resetPatterns) {
		final List<String> topics = topicStore.maybeDecorateInternalSourceTopics(resetTopics);

		return Refac_TopicStore.buildPatternForOffsetResetTopics(topics, resetPatterns);
	}

	public void validateTopicNotAlreadyRegistered(final String topic) {
		if (sourceTopicNames.contains(topic) || globalTopics.contains(topic)) {
			throw new TopologyException("Topic " + topic + " has already been registered by another source.");
		}

		if (sourceSink.topicMatchesPattern(topic)) {
			throw new TopologyException("Topic " + topic + " matches a Pattern already registered by another source.");
		}
	}

	private <T> void maybeAddToResetList(final Collection<T> earliestResets, final Collection<T> latestResets,
			final Topology.AutoOffsetReset offsetReset, final T item) {
		if (offsetReset != null) {
			switch (offsetReset) {
			case EARLIEST:
				earliestResets.add(item);
				break;
			case LATEST:
				latestResets.add(item);
				break;
			default:
				throw new TopologyException(String.format("Unrecognized reset format %s", offsetReset));
			}
		}
	}

}
