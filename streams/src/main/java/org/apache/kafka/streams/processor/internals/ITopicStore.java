package org.apache.kafka.streams.processor.internals;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public interface ITopicStore {

	List<String> decorateInternalSourceTopics(Collection<String> sourceTopics);

	boolean containsTopic(String topicName);

	String decorateTopic(String topic);

	Map<Integer, Set<String>> nodeGroups();

	Pattern topicForPattern(String topic);

	boolean hasPatternForTopic(String topic);

	void addPatternForTopic(String update, Pattern pattern);

}
