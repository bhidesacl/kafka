package org.apache.kafka.streams.processor.internals.nf;

import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.Processor;
import org.apache.kafka.streams.processor.internals.ProcessorNode;

public class ProcessorNodeFactory extends NodeFactory {

	private final ProcessorSupplier<?, ?> supplier;
	private final Set<String> stateStoreNames = new HashSet<>();

	public ProcessorNodeFactory(final String name, final String[] predecessors, final ProcessorSupplier<?, ?> supplier) {
		super(name, predecessors.clone());
		this.supplier = supplier;
	}

	public void addStateStore(final String stateStoreName) {
		stateStoreNames.add(stateStoreName);
	}

	@Override
	public ProcessorNode build() {
		return new ProcessorNode<>(name, supplier.get(), stateStoreNames);
	}

	@Override
	public Processor describe() {
		return new Processor(name, new HashSet<>(stateStoreNames));
	}

	public Set<String> getStateStoreNames() {
		return stateStoreNames;
	}

}
