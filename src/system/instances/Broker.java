package system.instances;

import system.data.BusLine;
import system.data.BusPosition;
import system.data.Message;
import system.data.Topic;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Broker implements Serializable {

    private String ipAddress;
    private int port;
    Topic topic;
    Publisher publisher;
    Broker broker;
    BrokerInfo brokerInfo;

    Hashtable<Topic, Set<Subscriber>> registeredSubscribers = new Hashtable<>();
    Set<Publisher> registeredPublishers = new HashSet<>();

    List<BusLine> busLines = new ArrayList<>();

    private Hashtable<Broker, HashSet<Topic>> mapOfBrokersResponsibilityLine = new Hashtable<>();
    private HashSet<Topic> brokerTopics = new HashSet<>();

    List<Broker> brokersCluster = new ArrayList<>();

    public Broker(String ipAddress, int port) {
        this.ipAddress = ipAddress;
        this.port = port;
    }

    public Broker(Broker broker) {
        this.broker = broker;
    }

    public Broker(int port) {
        this.port = port;
    }

    public static void main(String[] args) {

        Broker broker = new Broker("192.168.1.2", Integer.parseInt(args[0]));
        broker.init();
        broker.openServer();

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

                    // TODO na tsekarw an to topic pou thelei na kanei register o Publisher kai kala


                }

                else if (flag == 1) { // handle multiple push messages using thread

                    MultiplePushHandler pushHandler = new MultiplePushHandler(in, registeredSubscribers);
                    pushHandler.start();
                }

                else if (flag == 2) {

                    System.out.println("Broker " + this + " accepted a greeting and is returning the whole info");

                    brokerInfo = new BrokerInfo(brokersCluster, mapOfBrokersResponsibilityLine);

                    out.writeObject(brokerInfo);
                    out.flush();

                }

                else if (flag == 3) {

                    Subscriber subscriber = (Subscriber)in.readObject();

                    Topic topic = (Topic)in.readObject();

                    registerSubscriberForTopic(subscriber, topic);
                    System.out.println("Current registered " + registeredSubscribers);

                    out.writeObject("OK");
                    out.flush();

                }

            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
                providerSocket.close();

            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    private void sendMessage(Subscriber subscriber, Message message) {
        Socket requestSocket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;

        try {
            requestSocket = new Socket(subscriber.getAddr(), subscriber.getPort());

            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            int flagRegister = 4; // send flag 4 to subscriber that registered for the message i have in order to signal subscriber that a pull is happening

            try {

                out.writeInt(flagRegister);
                out.flush();

                out.writeObject(message); // send the message to the subscriber
                out.flush();

            } catch(Exception classNot){
                System.err.println("data received in unknown format");
                classNot.printStackTrace();
            }
        } catch (UnknownHostException unknownHost) {
            System.err.println("You are trying to connect to an unknown host!");
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            try {
                in.close();
                out.close();
                requestSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            } catch (Exception e) {
                System.err.println("Subscriber experienced error. This will not affect the runtime of broker ");
            }
        }
    }

    private void registerSubscriberForTopic(Subscriber subscriber, Topic topic) {
        Broker broker = hashTopic(topic);

        if(broker.equals(this)){
            // find the existing set of subscribers for this topic and add a new incoming subscriber
            // if null then create one with one element(the incoming subscriber) and add it to the hashTable
            if(!registeredSubscribers.containsKey(topic)) {
                Set<Subscriber> mySet = new HashSet<>();
                mySet.add(subscriber);
                registeredSubscribers.put(topic, mySet);
            } else {
                Set<Subscriber> existingSet = registeredSubscribers.get(topic);
                existingSet.add(subscriber);
                registeredSubscribers.put(topic,existingSet);
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

        for(Broker broker : brokersCluster) {
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

        Broker broker = brokersCluster.get(nodeId);

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


        //String brokersFile = "./Dataset/DS_project_dataset/BrokersList.txt";
        String brokersFile = "C:\\Users\\nikos\\workspace\\aueb\\distributed systems\\ds-project-2019\\Dataset\\DS_project_dataset\\BrokersList.txt";

        // read file into stream, try-with-resources
        try (Stream<String> stream = Files.lines(Paths.get(brokersFile))) {

            stream.map(line -> {
                String[] fields = line.split(",");
                Broker broker = new Broker(fields[0], Integer.parseInt(fields[1]));
                return broker; })
                    .forEach(line -> brokersCluster.add(line));

        } catch(IOException e) {
            e.printStackTrace();
        }

        // vale ola ta topics apo to arxeio se mia lista
        busLines = findAllTopicsFromBusLinesFile();

        List<Topic> allTopics = busLines.stream().map(busLine -> {
            Topic topic = new Topic(busLine.getLineId());
            return topic;
        }).collect(Collectors.toList());

        // gia kathe busLine(topic diladi) kalese ti hashtopic gia na mas epistrepsei poios broker einai
        // kai apothikeuse ta se mia domi. sto map. an uparxei hdh broker tote tha kanei put sto hashset pou uparxei hdh.

        for(Topic topic : allTopics) {
            Broker broker = hashTopic(topic);

            if(!mapOfBrokersResponsibilityLine.containsKey(broker)) {
                HashSet<Topic> mySet = new HashSet<>();
                mySet.add(topic);
                mapOfBrokersResponsibilityLine.put(broker, mySet);
            } else {
                HashSet<Topic> existingSet = mapOfBrokersResponsibilityLine.get(broker);
                existingSet.add(topic);
                mapOfBrokersResponsibilityLine.put(broker,existingSet);
            }

        }

        System.out.println(mapOfBrokersResponsibilityLine);

        populateMyBrokerTopics();

        System.out.println("brokerTopics " + brokerTopics);

    }

    public boolean equals(Object o) {
        if (o == null){
            return false;
        }
        Broker other = (Broker)o;

        return other.getIpAddress().equals(this.getIpAddress()) && other.getPort() == this.getPort();
    }

    private void populateMyBrokerTopics() {
        // pare to port kai to mapOfBrokersResponsibilityLine kai gemise to brokerTopics
        for(Broker broker : mapOfBrokersResponsibilityLine.keySet()) {
            if(this.equals(broker)) {
                HashSet<Topic> temp = mapOfBrokersResponsibilityLine.get(broker);
                brokerTopics.addAll(temp);
            }
        }

    }

    private List<BusLine> findAllTopicsFromBusLinesFile() {


        String busLinesFile = "C:\\Users\\nikos\\workspace\\aueb\\distributed systems\\ds-project-2019\\Dataset\\DS_project_dataset\\busLinesNew.txt";
        //String busLinesFile = "./Dataset/DS_project_dataset/busLinesNew.txt";
        List<BusLine> allBusLines = new ArrayList<>();

        //read file into stream, try-with-resources

        try (Stream<String> stream = Files.lines(Paths.get(busLinesFile))) {

            stream.map(line -> {
                String[] fields = line.split(",");
                BusLine myBusLine = new BusLine(fields[0], fields[1], fields[2]);
                return myBusLine; })
                    .forEach(busLineline -> allBusLines.add(busLineline));

        } catch(IOException e){
            e.printStackTrace();
        }

        return allBusLines;

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
