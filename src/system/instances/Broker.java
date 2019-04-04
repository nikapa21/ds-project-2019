package system.instances;

import system.data.Topic;

import java.util.ArrayList;
import java.util.List;

public interface Broker extends Node{

    List<Subscriber> registeredSubscribers = new ArrayList<>();
    List<Subscriber> registeredPublishers = new ArrayList<>();

    void calculateKeys();
    MyPublisher acceptConnection(MyPublisher publisher);
    Subscriber acceptConnection(Subscriber subscriber);
    void notifyPublisher(String msg);
    void pull(Topic topic);

}
