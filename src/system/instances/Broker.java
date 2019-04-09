package system.instances;

import system.data.Message;
import system.data.Topic;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class Broker implements Serializable {

    private String ipAddress;
    private int port;
    Topic topic;
    Publisher publisher;
    Broker broker;
    BrokerInfo brokerInfo;

    Set<Subscriber> registeredSubscribers = new HashSet<>();
    Set<Publisher> registeredPublishers = new HashSet<>();

    private Hashtable<Broker, HashSet<Topic>> listOfBrokersResponsibilityLine = new Hashtable<>();
    private HashSet<Topic> brokerTopics = new HashSet<>();

    public final static List<Broker> brokers = new ArrayList<>();

    public Broker(String ipAddress, int port) {
        this.ipAddress = ipAddress;
        this.port = port;
    }

    public static void main(String[] args) {

        Broker broker = new Broker("localhost", Integer.parseInt(args[0]));
        broker.init();
        broker.openServer();

        System.out.println("AEK");
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

                if (flag == 0) {

                    Publisher publisher = (Publisher)in.readObject();
                    // TODO register Publisher if not registered already
                    // registeredPublishers.add(publisher);
                    // System.out.println("Registered Publishers list " + registeredPublishers);

                    Topic topic = (Topic)in.readObject();
                    System.out.println("Broker " + this + " accepted a registration from publisher " + publisher + " for the topic " + topic);

                    // apo to hash topic tis publisher kserw idi oti egw eimai upeuthinos gia to topic pou mou irthe.
                    // to topic auto tha to kanw add sti lista topics kai meta
                    // tha valw to broker (diladi emena) sto map listOfBrokersResponsibility ws key, kai ws value tha valw ta topics gia ta opoia eimai upeuthinos

                    brokerTopics.add(topic);
                    listOfBrokersResponsibilityLine.put(this, brokerTopics);
                    System.out.println(listOfBrokersResponsibilityLine);

                }

                else if (flag == 1) {
                    publisher = (Publisher)in.readObject();
                    Message message = (Message)in.readObject();
                    System.out.println("Received push message from publisher " + publisher + ". Message: " + message);

//                    brokerTopics.add(topic);
//                    listOfBrokersResponsibilityLine.put(this, brokerTopics);

                }

                else if (flag == 2) {
                    Subscriber subscriber = (Subscriber)in.readObject();
                    // TODO register Subscriber if not registered already
                    // registeredSubscribers.add(subscriber);

                    Topic topic = (Topic)in.readObject();
                    System.out.println("Broker " + this + " accepted a greeting from subscriber " + subscriber);

                    brokerInfo = new BrokerInfo(brokers, listOfBrokersResponsibilityLine);

                    out.writeObject(brokerInfo);
                    out.flush();

                    // System.out.println("Broker " + this + " sent to subscriber " + subscriber + " a BrokerInfo " + brokerInfo);

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

//    private void registerPublisher(Publisher publisher) {
//        registeredPublishers.add(publisher);
//    }

//    private void registerSubscriber(Subscriber subscriber) {
//        boolean subscriberRegistered = false;
//        if (registeredSubscribers.size() == 0){
//            registeredSubscribers.add(subscriber);
//            subscriberRegistered = true;
//            System.out.println("New subscriber registered " + subscriber);
//        }
//        for (int i=0; i<registeredSubscribers.size(); i++){
//            if (subscriber.equals(registeredSubscribers.get(i))){
//                System.out.println("Subscriber already registered ");
//                subscriberRegistered = true;
//                break;
//            }
//        }
//        if(subscriberRegistered == false){
//            registeredSubscribers.add(subscriber);
//            System.out.println("New subscriber registered " + subscriber);
//        }
//    }

    public Broker hashTopic(Topic topic) {

        String busLineId = topic.getBusLine();
        String sha1Hash = null;// hash the name of file with sha1
        List<Integer> brokerHashesList = new ArrayList<>();

        try {
            sha1Hash = HashGenerator.generateSHA1(busLineId);
        } catch (HashGenerationException e) {
            e.printStackTrace();
        }
        int publisherKey = new BigInteger(sha1Hash, 16).intValue(); //convert the hex to big int
        int publisherModKey = Math.abs(publisherKey % 64);
        int brokerKey=0;
        int brokerModKey=0;

        for(Broker broker : Broker.brokers) {
            String brokerHash = null;// hash the name of file with sha1
            try {
                Broker mybroker = broker;
                brokerHash = HashGenerator.generateSHA1(mybroker.getIpAddress()+mybroker.getPort());
            } catch (HashGenerationException e) {
                e.printStackTrace();
            }
            brokerKey = new BigInteger(brokerHash, 16).intValue(); //convert the hex to big int
            brokerModKey = Math.abs(brokerKey % 64);

            //System.out.println(brokerKey + " " + brokerModKey + " " + publisherKey + " " + publisherModKey);
            brokerHashesList.add(brokerModKey);
        }

        int minDistance = 9999;
        int nodeId = 0;

        for (int i = 0; i < brokerHashesList.size(); i++) { //send the file in the correct(by id) node

            // System.out.println(brokerHashesList.get(i));
            if((Math.abs(publisherModKey - brokerHashesList.get(i))) < minDistance){
                minDistance = Math.abs(publisherModKey - brokerHashesList.get((i)));
                nodeId = i;
            }
        }

        Broker broker = Broker.brokers.get(nodeId);

//        System.out.println("MinDistance is " + minDistance + " and broker node that should be chosen is " + broker + " with id " + nodeId);
//        System.out.println();

        return broker;
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

    public void init(){

        // get Broker List

            String brokersFile = "./Dataset/DS_project_dataset/BrokersList.txt";

            // read file into stream, try-with-resources
            try (Stream<String> stream = Files.lines(Paths.get(brokersFile))) {

                stream.map(line -> {
                    String[] fields = line.split(",");
                    Broker broker = new Broker(fields[0], Integer.parseInt(fields[1]));
                    return broker; })
                        .forEach(line -> Broker.brokers.add(line));

            } catch(IOException e) {
                e.printStackTrace();
            }

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

    public void calculateKeys(Topic topic, Broker broker) {

    }

    public Publisher acceptConnection(Publisher publisher) { return  null; }

    public Subscriber acceptConnection(Subscriber subscriber) {
        return null;
    }

    public void notifyPublisher(String msg) {

    }

    public void pull(Topic topic) {

    }

    public String toString() {
        return "Broker{" +
                "ipAddress='" + ipAddress + '\'' +
                ", port=" + port +
                '}';
    }
}
