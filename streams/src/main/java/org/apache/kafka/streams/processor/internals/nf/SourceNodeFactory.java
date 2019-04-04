package org.apache.kafka.streams.processor.internals.nf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.Source;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.Refac_SourceSink;
import org.apache.kafka.streams.processor.internals.Refac_TopicStore;
import org.apache.kafka.streams.processor.internals.SourceNode;

public class SourceNodeFactory extends NodeFactory {
	
	private static final String[] NO_PREDECESSORS = {};
	private final List<String> topics;
	private final Pattern pattern;
	private final Deserializer<?> keyDeserializer;
	private final Deserializer<?> valDeserializer;
	private final TimestampExtractor timestampExtractor;
	private Refac_TopicStore topicStore;
	private Refac_SourceSink sourceSink;

	public SourceNodeFactory(final String name, final String[] topics, final Pattern pattern,
			final TimestampExtractor timestampExtractor, final Deserializer<?> keyDeserializer,
			final Deserializer<?> valDeserializer, final Refac_TopicStore topicStore,
			final Refac_SourceSink sourceSink) {
		super(name, NO_PREDECESSORS);
		this.topics = topics != null ? Arrays.asList(topics) : new ArrayList<>();
		this.pattern = pattern;
		this.keyDeserializer = keyDeserializer;
		this.valDeserializer = valDeserializer;
		this.timestampExtractor = timestampExtractor;
		this.topicStore = topicStore;
		this.sourceSink = sourceSink;
	}

	public List<String> getTopics(final Collection<String> subscribedTopics) {
		// if it is subscribed via patterns, it is possible that the topic metadata has
		// not been updated
		// yet and hence the map from source node to topics is stale, in this case we
		// put the pattern as a place holder;
		// this should only happen for debugging since during runtime this function
		// should always be called after the metadata has updated.
		if (subscribedTopics.isEmpty()) {
			return Collections.singletonList(String.valueOf(pattern));
		}

		final List<String> matchedTopics = new ArrayList<>();
		for (final String update : subscribedTopics) {
			if (pattern == topicStore.topicForPattern(update)) {
				matchedTopics.add(update);
			} else if (topicStore.hasPatternForTopic(update) && isMatch(update)) {
				// the same topic cannot be matched to more than one pattern
				// TODO: we should lift this requirement in the future
				throw new TopologyException("Topic " + update + " is already matched for another regex pattern "
						+ topicStore.topicForPattern(update) + " and hence cannot be matched to this regex pattern "
						+ pattern + " any more.");
			} else if (isMatch(update)) {
				topicStore.addPatternForTopic(update, pattern);
				matchedTopics.add(update);
			}
		}
		return matchedTopics;
	}

	@Override
	public ProcessorNode build() {
		final List<String> sourceTopics = sourceSink.sourceTopicsForNode(name);

		// if it is subscribed via patterns, it is possible that the topic metadata has
		// not been updated
		// yet and hence the map from source node to topics is stale, in this case we
		// put the pattern as a place holder;
		// this should only happen for debugging since during runtime this function
		// should always be called after the metadata has updated.
		if (sourceTopics == null) {
			return new SourceNode<>(name, Collections.singletonList(String.valueOf(pattern)), timestampExtractor,
					keyDeserializer, valDeserializer);
		} else {
			return new SourceNode<>(name, topicStore.maybeDecorateInternalSourceTopics(sourceTopics),
					timestampExtractor, keyDeserializer, valDeserializer);
		}
	}

	private boolean isMatch(final String topic) {
		return pattern.matcher(topic).matches();
	}

	public Pattern getPattern() {
		return pattern;
	}

	public List<String> getTopics() {
		return topics;
	}

	@Override
	Source describe() {
		return new Source(name, new HashSet<>(topics), pattern);
	}
}
