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
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.internals.Refac_InternalTopologyBuilder.GlobalStore;
import org.apache.kafka.streams.processor.internals.Refac_InternalTopologyBuilder.NodeFactory;
import org.apache.kafka.streams.processor.internals.Refac_InternalTopologyBuilder.ProcessorNodeFactory;
import org.apache.kafka.streams.processor.internals.Refac_InternalTopologyBuilder.SourceNodeFactory;
import org.apache.kafka.streams.processor.internals.Refac_InternalTopologyBuilder.SubscriptionUpdates;
import org.apache.kafka.streams.processor.internals.Refac_InternalTopologyBuilder.TopologyDescription;

public class Refac_SourceSink {
	// map from source processor names to subscribed topics (without application-id
	// prefix for internal topics)
	private final Map<String, List<String>> nodeToSourceTopics = new HashMap<>();

	// map from source processor names to regex subscription patterns
	private final Map<String, Pattern> nodeToSourcePatterns = new LinkedHashMap<>();

	// map from sink processor names to subscribed topic (without application-id
	// prefix for internal topics)
	private final Map<String, String> nodeToSinkTopic = new HashMap<>();

	// map from state store names to all the topics subscribed from source
	// processors that
	// are connected to these state stores
	private final Map<String, Set<String>> stateStoreNameToSourceTopics = new HashMap<>();

	// map from state store names to all the regex subscribed topics from source
	// processors that
	// are connected to these state stores
	private final Map<String, Set<Pattern>> stateStoreNameToSourceRegex = new HashMap<>();

	// groups of source processors that need to be copartitioned
	private final List<Set<String>> copartitionSourceGroups = new ArrayList<>();

	private Map<String, NodeFactory> nodeFactories;

	private SubscriptionUpdates subscriptionUpdates;

	private Refac_GlobalTopics globalTopics;

	private QuickUnion<String> nodeGrouper;

	public Refac_SourceSink(Map<String, NodeFactory> nodeFactories, SubscriptionUpdates subscriptionUpdates,
			Refac_GlobalTopics globalTopics, QuickUnion<String> nodeGrouper) {
		this.nodeFactories = nodeFactories;
		this.subscriptionUpdates = subscriptionUpdates;
		this.globalTopics = globalTopics;
		this.nodeGrouper = nodeGrouper;
	}

	public List<String> sourceTopicsForNode(String name) {
		return nodeToSourceTopics.get(name);
	}

	public void addSourceTopicsToNode(String name, List<String> topics) {
		nodeToSourceTopics.put(name, topics);
	}

	public void addSourcePatternToNode(String name, Pattern topicPattern) {
		nodeToSourcePatterns.put(name, topicPattern);
	}

	public void addSinkTopicToNode(String name, String topic) {
		nodeToSinkTopic.put(name, topic);
	}

	public boolean hasSinkTopicForNode(String name) {
		return nodeToSinkTopic.containsKey(name);
	}

	public boolean topicMatchesPattern(String topic) {
		boolean result = false;
		for (final Pattern pattern : nodeToSourcePatterns.values()) {
			if (pattern.matcher(topic).matches()) {
				result = true;
				break;
			}
		}

		return result;
	}

	public Pattern sourceTopicPattern(Refac_TopicStore topicStore) {
		final List<String> allSourceTopics = new ArrayList<>();
		if (!nodeToSourceTopics.isEmpty()) {
			for (final List<String> topics : nodeToSourceTopics.values()) {
				allSourceTopics.addAll(topicStore.maybeDecorateInternalSourceTopics(topics));
			}
		}
		Collections.sort(allSourceTopics);

		return Refac_TopicStore.buildPatternForOffsetResetTopics(allSourceTopics, nodeToSourcePatterns.values());
	}

	public Map<String, List<String>> stateStoreNameToSourceTopics(Refac_TopicStore topicStore) {
		final Map<String, List<String>> results = new HashMap<>();
		for (final Map.Entry<String, Set<String>> entry : stateStoreNameToSourceTopics.entrySet()) {
			results.put(entry.getKey(), topicStore.maybeDecorateInternalSourceTopics(entry.getValue()));
		}
		return results;
	}

	public void copartitionSources(Collection<String> sourceNodes) {
		copartitionSourceGroups.add(Collections.unmodifiableSet(new HashSet<>(sourceNodes)));
	}

	public Collection<Set<String>> copartitionGroups(Refac_TopicStore topicStore) {
		final List<Set<String>> list = new ArrayList<>(copartitionSourceGroups.size());
		for (final Set<String> nodeNames : copartitionSourceGroups) {
			final Set<String> copartitionGroup = new HashSet<>();
			for (final String node : nodeNames) {
				final List<String> topics = nodeToSourceTopics.get(node);
				if (topics != null) {
					copartitionGroup.addAll(topicStore.maybeDecorateInternalSourceTopics(topics));
				}
			}
			list.add(Collections.unmodifiableSet(copartitionGroup));
		}
		return Collections.unmodifiableList(list);
	}

	public String sinkTopicForNode(String node) {
		return nodeToSinkTopic.get(node);
	}

	public void setRegexMatchedTopicsToSourceNodes() {
		if (subscriptionUpdates.hasUpdates()) {
			for (final Map.Entry<String, Pattern> stringPatternEntry : nodeToSourcePatterns.entrySet()) {
				final SourceNodeFactory sourceNode = (SourceNodeFactory) nodeFactories.get(stringPatternEntry.getKey());
				// need to update nodeToSourceTopics with topics matched from given regex
				nodeToSourceTopics.put(stringPatternEntry.getKey(),
						sourceNode.getTopics(subscriptionUpdates.getUpdates()));
			}
		}
	}

	public void setRegexMatchedTopicToStateStore() {
		if (subscriptionUpdates.hasUpdates()) {
			for (final Map.Entry<String, Set<Pattern>> storePattern : stateStoreNameToSourceRegex.entrySet()) {
				final Set<String> updatedTopicsForStateStore = new HashSet<>();
				for (final String subscriptionUpdateTopic : subscriptionUpdates.getUpdates()) {
					for (final Pattern pattern : storePattern.getValue()) {
						if (pattern.matcher(subscriptionUpdateTopic).matches()) {
							updatedTopicsForStateStore.add(subscriptionUpdateTopic);
						}
					}
				}
				if (!updatedTopicsForStateStore.isEmpty()) {
					final Collection<String> storeTopics = stateStoreNameToSourceTopics.get(storePattern.getKey());
					if (storeTopics != null) {
						updatedTopicsForStateStore.addAll(storeTopics);
					}
					stateStoreNameToSourceTopics.put(storePattern.getKey(),
							Collections.unmodifiableSet(updatedTopicsForStateStore));
				}
			}
		}
	}

	public void describeGlobalStore(final TopologyDescription description, final Set<String> nodes, final int id) {
		final Iterator<String> it = nodes.iterator();
		while (it.hasNext()) {
			final String node = it.next();

			if (isGlobalSource(node)) {
				// we found a GlobalStore node group; those contain exactly two node:
				// {sourceNode,processorNode}
				it.remove(); // remove sourceNode from group
				final String processorNode = nodes.iterator().next(); // get remaining processorNode

				description
						.addGlobalStore(new GlobalStore(
								node, processorNode, ((ProcessorNodeFactory) nodeFactories.get(processorNode))
										.getStateStoreNames().iterator().next(),
								nodeToSourceTopics.get(node).get(0), id));
				break;
			}
		}
	}

	private boolean isGlobalSource(final String nodeName) {
		final NodeFactory nodeFactory = nodeFactories.get(nodeName);

		if (nodeFactory instanceof SourceNodeFactory) {
			final List<String> topics = ((SourceNodeFactory) nodeFactory).getTopics();
			return topics != null && topics.size() == 1 && globalTopics.contains(topics.get(0));
		}
		return false;
	}
	
	public void connectStateStoreNameToSourceTopicsOrPattern(Map<String, NodeFactory> nodeFactories,
			final String stateStoreName, final ProcessorNodeFactory processorNodeFactory) {
// we should never update the mapping from state store names to source topics if the store name already exists
// in the map; this scenario is possible, for example, that a state store underlying a source KTable is
// connecting to a join operator whose source topic is not the original KTable's source topic but an internal repartition topic.

		if (stateStoreNameToSourceTopics.containsKey(stateStoreName)
				|| stateStoreNameToSourceRegex.containsKey(stateStoreName)) {
			return;
		}

		final Set<String> sourceTopics = new HashSet<>();
		final Set<Pattern> sourcePatterns = new HashSet<>();
		final Set<SourceNodeFactory> sourceNodesForPredecessor = findSourcesForProcessorPredecessors(nodeFactories,
				processorNodeFactory.predecessors);

		for (final SourceNodeFactory sourceNodeFactory : sourceNodesForPredecessor) {
			if (sourceNodeFactory.getPattern() != null) {
				sourcePatterns.add(sourceNodeFactory.getPattern());
			} else {
				sourceTopics.addAll(sourceNodeFactory.getTopics());
			}
		}

		if (!sourceTopics.isEmpty()) {
			stateStoreNameToSourceTopics.put(stateStoreName, Collections.unmodifiableSet(sourceTopics));
		}

		if (!sourcePatterns.isEmpty()) {
			stateStoreNameToSourceRegex.put(stateStoreName, Collections.unmodifiableSet(sourcePatterns));
		}

	}
	
	private Set<SourceNodeFactory> findSourcesForProcessorPredecessors(Map<String, NodeFactory> nodeFactories,
			final String[] predecessors) {
		final Set<SourceNodeFactory> sourceNodes = new HashSet<>();
		for (final String predecessor : predecessors) {
			final NodeFactory nodeFactory = nodeFactories.get(predecessor);
			if (nodeFactory instanceof SourceNodeFactory) {
				sourceNodes.add((SourceNodeFactory) nodeFactory);
			} else if (nodeFactory instanceof ProcessorNodeFactory) {
				sourceNodes.addAll(findSourcesForProcessorPredecessors(nodeFactories,
						((ProcessorNodeFactory) nodeFactory).predecessors));
			}
		}
		return sourceNodes;
	}
	
	public Map<Integer, Set<String>> makeNodeGroups() {
		final Map<Integer, Set<String>> nodeGroups = new LinkedHashMap<>();
		final Map<String, Set<String>> rootToNodeGroup = new HashMap<>();

		int nodeGroupId = 0;

		// Go through source nodes first. This makes the group id assignment easy to
		// predict in tests
		final Set<String> allSourceNodes = new HashSet<>(nodeToSourceTopics.keySet());
		allSourceNodes.addAll(nodeToSourcePatterns.keySet());

		for (final String nodeName : Utils.sorted(allSourceNodes)) {
			nodeGroupId = putNodeGroupName(nodeName, nodeGroupId, nodeGroups, rootToNodeGroup);
		}

		// Go through non-source nodes
		for (final String nodeName : Utils.sorted(nodeFactories.keySet())) {
			if (!nodeToSourceTopics.containsKey(nodeName)) {
				nodeGroupId = putNodeGroupName(nodeName, nodeGroupId, nodeGroups, rootToNodeGroup);
			}
		}

		return nodeGroups;
	}
	
	private int putNodeGroupName(final String nodeName, final int nodeGroupId,
			final Map<Integer, Set<String>> nodeGroups, final Map<String, Set<String>> rootToNodeGroup) {
		int newNodeGroupId = nodeGroupId;
		final String root = nodeGrouper.root(nodeName);
		Set<String> nodeGroup = rootToNodeGroup.get(root);
		if (nodeGroup == null) {
			nodeGroup = new HashSet<>();
			rootToNodeGroup.put(root, nodeGroup);
			nodeGroups.put(newNodeGroupId++, nodeGroup);
		}
		nodeGroup.add(nodeName);
		return newNodeGroupId;
	}
}
