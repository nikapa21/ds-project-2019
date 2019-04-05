package system.instances;

import system.data.Message;
import system.data.Topic;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class Broker {

    List<Subscriber> registeredSubscribers = new ArrayList<>();
    List<Publisher> registeredPublishers = new ArrayList<>();

    public final static List<Broker> brokers = new ArrayList<>();

    private String ipAddress;
    private int port;

    public Broker(String ipAddress, int port) {
        this.ipAddress = ipAddress;
        this.port = port;
    }

    public static void main(String[] args) {

        new Broker("localhost", Integer.parseInt(args[0])).openServer();

    }

    public void openServer() {
        ServerSocket providerSocket = null;
        Socket connection = null;

        try {
            providerSocket = new ServerSocket(port);

            while (true) {
                connection = providerSocket.accept();

                ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(connection.getInputStream());
                int flag;

                flag = in.readInt();

                if (flag == 0){
                    Publisher publisher = (Publisher)in.readObject();
                    // TODO register publisher
                    Topic topic = (Topic)in.readObject();
                    System.out.println("Broker " + this + " accepted a registration from publisher " + publisher + " for the topic " + topic);
                }

                else if (flag == 1) {
                    Publisher publisher = (Publisher)in.readObject();
                    Message message = (Message)in.readObject();
                    System.out.println("Received push message from publisher " + publisher + ". Message: " + message);
                }

                in.close();
                out.close();
                connection.close();
            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                providerSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    public String toString() {
        return "Broker{" +
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

    public void init(int port){
        brokers.add(this);
    }

    public void connect() {

    }

    public void disconnect() {

    }

    public void updateNodes() {

    }

    public List<Broker> getBrokers() {
        return null;
    }

    public void calculateKeys() {

    }

//    public Publisher acceptConnection(Publisher publisher) {
//
//        Broker.registeredPublishers.add(publisher);
//        return publisher;
//
//    }

    public Subscriber acceptConnection(Subscriber subscriber) {
        return null;
    }

    public void notifyPublisher(String msg) {

    }

    public void pull(Topic topic) {

    }
}
