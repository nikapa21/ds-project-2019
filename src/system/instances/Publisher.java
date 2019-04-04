package system.instances;

import system.data.Topic;
import system.data.Value;

public interface Publisher extends Node {

     void getBrokerList();
     Broker hashTopic(Topic topic);
     void push(Topic topic, Value value);
     void notifyFailure(Broker broker);

}
