package org.apache.kafka.streams.processor.internals.nf;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.Sink;
import org.apache.kafka.streams.processor.internals.ITopicStore;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.SinkNode;
import org.apache.kafka.streams.processor.internals.StaticTopicNameExtractor;

public class SinkNodeFactory<K, V> extends NodeFactory {
	private final Serializer<K> keySerializer;
	private final Serializer<V> valSerializer;
	private final StreamPartitioner<? super K, ? super V> partitioner;
	private final TopicNameExtractor<K, V> topicExtractor;
	private ITopicStore topicStore;

	public SinkNodeFactory(final String name, final String[] predecessors,
			final TopicNameExtractor<K, V> topicExtractor, final Serializer<K> keySerializer,
			final Serializer<V> valSerializer, final StreamPartitioner<? super K, ? super V> partitioner,
			final ITopicStore topicStore) {
		super(name, predecessors.clone());
		this.topicExtractor = topicExtractor;
		this.keySerializer = keySerializer;
		this.valSerializer = valSerializer;
		this.partitioner = partitioner;
		this.topicStore = topicStore;
	}

	@Override
	public ProcessorNode build() {
		if (topicExtractor instanceof StaticTopicNameExtractor) {
			final String topic = ((StaticTopicNameExtractor) topicExtractor).topicName;
			if (topicStore.containsTopic(topic)) {
				// prefix the internal topic name with the application id
				return new SinkNode<>(name, new StaticTopicNameExtractor<>(topicStore.decorateTopic(topic)),
						keySerializer, valSerializer, partitioner);
			} else {
				return new SinkNode<>(name, topicExtractor, keySerializer, valSerializer, partitioner);
			}
		} else {
			return new SinkNode<>(name, topicExtractor, keySerializer, valSerializer, partitioner);
		}
	}

	public TopicNameExtractor<K, V> getTopicExtractor() {
		return topicExtractor;
	}

	@Override
	public Sink describe() {
		return new Sink(name, topicExtractor);
	}
}
