package org.apache.kafka.streams.processor.internals;

import java.util.HashSet;
import java.util.Set;

public class Refac_GlobalTopics {
	private final Set<String> globalTopics = new HashSet<>();

	public void add(String topic) {
		globalTopics.add(topic);
	}

	public boolean contains(String topic) {
		return globalTopics.contains(topic);
	}
}
