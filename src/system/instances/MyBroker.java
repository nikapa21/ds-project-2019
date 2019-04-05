package system.instances;

import system.data.Topic;

import java.util.List;

public class MyBroker implements Broker{

    private String ipAddress;
    private int port;
    private Broker broker;

    @Override
    public String toString() {
        return "MyBroker{" +
                "ipAddress='" + ipAddress + '\'' +
                ", port=" + port +
                '}';
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public MyBroker(String ipAddress, int port) {
        this.ipAddress = ipAddress;
        this.port = port;

    }

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

        Broker.registeredPublishers.add(publisher);
        return publisher;

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
