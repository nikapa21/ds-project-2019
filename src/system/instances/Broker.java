package system.instances;

import system.data.Topic;

import java.util.ArrayList;
import java.util.List;

public class Broker implements Node{

    List<Subscriber> registeredSubscribers = new ArrayList<>();
    List<Subscriber> registeredPublishers = new ArrayList<>();

//    void calculateKeys();
//    Publisher acceptConnection(Publisher publisher);
//    Subscriber acceptConnection(Subscriber subscriber);
//    void notifyPublisher(String msg);
//    void pull(Topic topic);

    void init(int port){
        brokers.add(this);
    }

//    void connect();
//    void disconnect();
//    void updateNodes();

}
