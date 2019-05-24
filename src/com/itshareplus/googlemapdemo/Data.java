package com.itshareplus.googlemapdemo;

import java.io.Serializable;

public class Data implements Serializable {

	Topic topic;
	Value value;

	public Data(Topic topic, Value value) {
		this.topic = topic;
		this.value = value;
	}

	public Topic getTopic() {
		return topic;
	}

	public void setTopic(Topic topic) {
		this.topic = topic;
	}

	public Value getValue() {
		return value;
	}

	public void setValue(Value value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "Data{" +
				"topic=" + topic +
				", value=" + value +
				'}';
	}
}
