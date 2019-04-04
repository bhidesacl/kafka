package org.apache.kafka.streams.processor.internals.nf;

import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.AbstractNode;

public abstract class NodeFactory {
	protected final String name;
	protected final String[] predecessors;

	NodeFactory(final String name, final String[] predecessors) {
		this.name = name;
		this.predecessors = predecessors;
	}

	public abstract ProcessorNode build();

	public abstract AbstractNode describe();

	public String[] getPredecessors() {
		return predecessors;
	}

	public String getName() {
		return name;
	}
}
