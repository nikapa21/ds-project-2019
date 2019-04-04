package system.instances;

import system.data.Topic;

import java.util.List;

public class MyBroker implements Broker{

    public void init(int port){
        brokers.add(this);
    }

    @Override
    public void connect() {

    }

    @Override
    public void disconnect() {

    }

    @Override
    public void updateNodes() {

    }

    @Override
    public List<Broker> getBrokers() {
        return null;
    }

    @Override
    public void calculateKeys() {

    }

    @Override
    public MyPublisher acceptConnection(MyPublisher publisher) {
        return null;
    }

    @Override
    public Subscriber acceptConnection(Subscriber subscriber) {
        return null;
    }

    @Override
    public void notifyPublisher(String msg) {

    }

    @Override
    public void pull(Topic topic) {

    }
}
