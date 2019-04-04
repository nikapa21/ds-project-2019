package system.instances;

import system.data.Topic;
import system.data.Value;

public interface Subscriber extends Node{
    void register(Broker broker, Topic topic);
    void disconnect(Broker broker, Topic topic);
    void visualiseData(Topic topic, Value value);
}
