package org.apache.kafka.streams.processor.internals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.AbstractNode;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.Subtopology;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.TopologyDescription;
import org.apache.kafka.streams.processor.internals.nf.NodeFactory;
import org.apache.kafka.streams.processor.internals.nf.SourceNodeFactory;

public class Refac_TopologyDescriptionGen {

	private final ITopicStore topicStore;
	private final Refac_SourceSink sourceSink;
	private final Map<String, NodeFactory> nodeFactories;
	private final Refac_GlobalTopics globalTopics;

	public Refac_TopologyDescriptionGen(ITopicStore topicStore, Refac_SourceSink sourceSink,
			Map<String, NodeFactory> nodeFactories, Refac_GlobalTopics globalTopics) {
		this.topicStore = topicStore;
		this.sourceSink = sourceSink;
		this.nodeFactories = nodeFactories;
		this.globalTopics = globalTopics;
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

	private boolean isGlobalSource(final String nodeName) {
		final NodeFactory nodeFactory = nodeFactories.get(nodeName);

		if (nodeFactory instanceof SourceNodeFactory) {
			final List<String> topics = ((SourceNodeFactory) nodeFactory).getTopics();
			return topics != null && topics.size() == 1 && globalTopics.contains(topics.get(0));
		}
		return false;
	}

	private static void updateSize(final AbstractNode node, final int delta) {
		node.size += delta;

		for (final TopologyDescription.Node predecessor : node.predecessors()) {
			updateSize((AbstractNode) predecessor, delta);
		}
	}
}
