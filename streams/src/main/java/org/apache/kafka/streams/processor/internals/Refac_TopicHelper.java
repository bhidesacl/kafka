package org.apache.kafka.streams.processor.internals;

import java.util.Collection;
import java.util.regex.Pattern;

import org.apache.kafka.streams.processor.internals.Refac_TopicStore.StateStoreFactory;

public class Refac_TopicHelper {
	private static final Pattern EMPTY_ZERO_LENGTH_PATTERN = Pattern.compile("");

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

	public static InternalTopicConfig createChangelogTopicConfig(final StateStoreFactory factory, final String name) {
		if (factory.isWindowStore()) {
			final WindowedChangelogTopicConfig config = new WindowedChangelogTopicConfig(name, factory.logConfig());
			config.setRetentionMs(factory.retentionPeriod());
			return config;
		} else {
			return new UnwindowedChangelogTopicConfig(name, factory.logConfig());
		}
	}
}
