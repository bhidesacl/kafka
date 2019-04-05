package org.apache.kafka.streams.processor.internals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.SubscriptionUpdates;
import org.apache.kafka.streams.processor.internals.Refac_TopicStore.StateStoreFactory;
import org.apache.kafka.streams.processor.internals.nf.NodeFactory;
import org.apache.kafka.streams.processor.internals.nf.ProcessorNodeFactory;
import org.apache.kafka.streams.processor.internals.nf.SinkNodeFactory;
import org.apache.kafka.streams.processor.internals.nf.SourceNodeFactory;

public class Refac_NodeBuilder {
	private final Map<String, StateStore> globalStateStores;
	private final Map<String, String> storeToChangelogTopic;
	private final Map<String, StateStoreFactory> stateFactories;
	private String applicationId;
	private final Set<String> internalTopicNames;

	public Refac_NodeBuilder(Map<String, StateStore> globalStateStores, Map<String, String> storeToChangelogTopic,
			Map<String, StateStoreFactory> stateFactories, Set<String> internalTopicNames) {
		this.globalStateStores = globalStateStores;
		this.storeToChangelogTopic = storeToChangelogTopic;
		this.stateFactories = stateFactories;
		this.internalTopicNames = internalTopicNames;
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

	public String decorateTopic(final String topic) {
		if (applicationId == null) {
			throw new TopologyException("there are internal topics and " + "applicationId hasn't been set. Call "
					+ "setApplicationId first");
		}

		return applicationId + "-" + topic;
	}

	public void setApplicationId(String applicationId) {
		this.applicationId = applicationId;
		
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
}
