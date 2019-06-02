package com.itshareplus.googlemapdemo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Publisher extends Thread implements Serializable{

    String addr;
    int port;
    BrokerInfo brokerInfo;

    List<Topic> topics = new ArrayList<>();

    List<Value> publisherValues = new ArrayList<>();
    List<BusLine> publisherBusLines = new ArrayList<>();
    List<RouteCode> publisherRouteCodes = new ArrayList<>();
    List<BusPosition> publisherBusPositions = new ArrayList<>();

    List<Broker> brokersCluster = new ArrayList<>();

    public Publisher(String addr, int port) {
        this.addr = addr;
        this.port = port;
    }

    public Publisher(){}

    public static void main(String[] args) {
        Publisher publisher = new Publisher("192.168.1.4", Integer.parseInt(args[1]));

        // O publisher node κατά την έναρξη της λειτουργίας του θα πρέπει να γνωρίζει για
        //ποια  κλειδιά  είναι  υπεύθυνος  καθώς  επίσης  και  όλη  την  απαραίτητη πληροφορία  για  τους  brokers.

        // vres ola ta kleidia gia ta opoia eisai upeuthinos. Diavase apo ta arxeia to vehicleId kai ta topics pou prokuptoun analogws
        publisher.init(Integer.parseInt(args[0]));

        // mathe tin aparaititi pliroforia gia tous brokers diladi vres apo to arxeio olous tous brokers

        publisher.getBrokerList();

        // kai sugxronisou me enan apo autous wste na sou pei gia poia topics einai o kathe broker upeuthinos.
        // o publisher tha parei oli tin pliroforia gia to poios einai upeuthinos. xwris na kanei hashTopic.
        publisher.fetchAllTheBrokerInfo();

        for(Topic topic : publisher.topics) {
            Broker broker = publisher.findMyBrokerForMyTopic(topic); // prepei na to kanw gia kathe topic
            publisher.doTheRegister(broker, topic);
        }

        // kanoume push ola ta values pou exoume (idanika xrisimopoiwntas sleep)

        for(Value value : publisher.publisherValues) {
            Topic topic = new Topic(value.getBuslineId());
            Broker broker = publisher.hashTopic(topic);
            Data data = new Data(topic, value);
            publisher.pushTheMessageToBroker(broker, data);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    private Broker findMyBrokerForMyTopic(Topic topic) {
        Broker myBroker = null;

        for(Broker broker : brokerInfo.getListOfBrokersResponsibilityLine().keySet()) {
            HashSet<Topic> mySet = brokerInfo.getListOfBrokersResponsibilityLine().get(broker);
            if (mySet.contains(topic)) {
                // an to mySet exei to topic krata to key
                myBroker = broker;
                break;
            }
        }
        return myBroker;
    }

    private void fetchAllTheBrokerInfo() {
        Socket requestSocket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;

        try {
            requestSocket = new Socket("192.168.1.4", 7000);

            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            int flagRegister = 2; // send flag 2 to broker 7000 in order to fetch all info about brokers and responsibilities

            try {

                out.writeInt(flagRegister);
                out.flush();

                // perimenw na mathw poioi einai oi upoloipoi brokers kai gia poia kleidia einai upeuthinoi
                // diladi perimenw ena antikeimeno Info tis morfis {ListOfBrokers, <BrokerId, ResponsibilityLine>}
                String greetingMessage = (String) in.readObject();
                System.out.println(greetingMessage);

                brokerInfo = (BrokerInfo)in.readObject();
                System.out.println("Received from broker brokerinfo upon preregister: " + brokerInfo);


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
            }
        }
    }

    private void doTheRegister(Broker broker, Topic topic) {

        Socket requestSocket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;

        try {
            requestSocket = new Socket(InetAddress.getByName(broker.getIpAddress()), broker.getPort());

            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            int flagRegister = 0; // send flag 0 to register publisher

            try {

                out.writeInt(flagRegister);
                out.flush();

                out.writeObject(this); // send the publisher himself (this) to be registered
                out.flush();

                out.writeObject(topic);
                out.flush();

            } catch(Exception classNot){
                System.err.println("data received in unknown format");
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
            }
        }
    }

    public void pushTheMessageToBroker(Broker broker, Data data) {
            Socket requestSocket = null;
            ObjectOutputStream out = null;
            ObjectInputStream in = null;

            try {
                requestSocket = new Socket(InetAddress.getByName(broker.getIpAddress()), broker.getPort());

                out = new ObjectOutputStream(requestSocket.getOutputStream());
                in = new ObjectInputStream(requestSocket.getInputStream());

                int flagPush = 1; // send push data

                try {

                    out.writeInt(flagPush);
                    out.flush();

                    out.writeObject(this);
                    out.flush();

                    out.writeObject(data);
                    out.flush();

                } catch(Exception classNot){
                    System.err.println("data received in unknown format");
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
                }
            }
        }

    public void init(int vehicleId) { //10374

        // vres ola ta kleidia gia ta opoia eisai upeuthinos
        initiateTopicAndValueList(vehicleId);

    }

    /***** PUBLISHER METHODS *****/

    public void getBrokerList() {

        //String brokersFile = "./Dataset/DS_project_dataset/BrokersList.txt";
        String brokersFile = "C:\\Users\\GiorgosFassas\\workspace\\aueb\\distributed systems\\ds-project-2019\\Dataset\\DS_project_dataset\\BrokersList.txt";

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

    }

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

    public void notifyFailure(Broker broker) {

    }

    private void initiateTopicAndValueList(int vehicleId) {

        // Me vasi to vehicle id vres ola ta bus position objects apo to arxeio me ta bus positions
        findFromBusPositionsFile(vehicleId);

        // Vres ola ta distinct route codes apo ta parapanw bus position objects
        List<String> distinctRouteCodes = publisherBusPositions.stream().map(BusPosition::getRouteCode).distinct().collect(Collectors.toList());

        // Apo ta distinct route codes Strings vres ola ta route code objects apo to arxeio me ta route codes
        findFromRouteCodesFile(distinctRouteCodes);

        // Vres ola ta distinct line codes apo ta parapanw route code objects
        List<String> distinctLineCodes = publisherRouteCodes.stream().map(RouteCode::getLineCode).distinct().collect(Collectors.toList());

        // Apo ta distinct bus lines Strings vres ola ta bus line objects apo to arxeio me ta bus lines
        findFromBusLinesFile(distinctLineCodes);

        findValueFromBusPositionsList();
        for (BusLine busline : publisherBusLines) {
            populateAllNullValues(busline);
        }

        System.out.println(publisherValues.toString());


        System.out.print("Vehicle with id " + vehicleId + " is responsible for the following lines/topics: ");
        for(BusLine busLine: publisherBusLines) {
            topics.add(new Topic(busLine.getLineId()));
            System.out.print(busLine.getLineId() + " ");
        }
        System.out.println();
    }

    private void populateAllNullValues(BusLine busline) {
        for (Value value : publisherValues) {
            if (value.getLineNumber().equals(busline.getLineCode())) {
                value.setBuslineId(busline.getLineId());
                value.setLineName(busline.getDescriptionEnglish());
            }
        }
    }

    private void findValueFromBusPositionsList() {
        for(BusPosition busPosition : publisherBusPositions){ // tha mporouse na ginei kai me stream.map() se java 8
            publisherValues.add(new Value(busPosition.getLineCode(), busPosition.getRouteCode(), busPosition.getVehicleId()
                    , null, null, busPosition.getTimestampOfBusPosition(), busPosition.getLatitude(), busPosition.getLongitude()));
        }
    }

    private void findFromBusLinesFile(List<String> busLines) {

        // String busLinesFile = "./Dataset/DS_project_dataset/busLinesNew.txt";
        String busLinesFile = "C:\\Users\\GiorgosFassas\\workspace\\aueb\\distributed systems\\ds-project-2019\\Dataset\\DS_project_dataset\\busLinesNew.txt";

        //read file into stream, try-with-resources
        for (String busLine : busLines) {
            try (Stream<String> stream = Files.lines(Paths.get(busLinesFile))) {

                stream.map(line -> {
                            String[] fields = line.split(",");
                            BusLine myBusLine = new BusLine(fields[0], fields[1], fields[2]);
                            return myBusLine; })
                       .filter(busLineline -> busLineline.getLineCode().equals(String.valueOf(busLine)))
                       .forEach(busLineline -> publisherBusLines.add(busLineline));

            } catch(IOException e){
                e.printStackTrace();
            }

        }
    }

    private void findFromRouteCodesFile(List<String> routeCodes) {

        // String routeCodesFile = "./Dataset/DS_project_dataset/RouteCodesNew.txt";
        String routeCodesFile = "C:\\Users\\GiorgosFassas\\workspace\\aueb\\distributed systems\\ds-project-2019\\Dataset\\DS_project_dataset\\RouteCodesNew.txt";

        //read file into stream, try-with-resources
        for(String routeCode : routeCodes) {
            try (Stream<String> stream = Files.lines(Paths.get(routeCodesFile))) {

                stream.map(line -> {
                            String[] fields = line.split(",");
                            RouteCode routeCodeObject = new RouteCode(fields[1], fields[0], fields[3]);
                            return routeCodeObject; })
                       .filter(routeCodeline -> routeCodeline.getRouteCode().equals(String.valueOf(routeCode)))
                       .forEach(routeCodeline -> publisherRouteCodes.add(routeCodeline));

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void findFromBusPositionsFile(int vehicleId) {

        // String busPositionsFile = "./Dataset/DS_project_dataset/busPositionsNew.txt";
        String busPositionsFile = "C:\\Users\\GiorgosFassas\\workspace\\aueb\\distributed systems\\ds-project-2019\\Dataset\\DS_project_dataset\\busPositionsNew.txt";

        // read file into stream, try-with-resources
        try (Stream<String> stream = Files.lines(Paths.get(busPositionsFile))) {

            stream.map(line -> {
                        String[] fields = line.split(",");
                        BusPosition busPosition = new BusPosition(fields[0], fields[1], fields[2], Double.parseDouble(fields[3]), Double.parseDouble(fields[4]), fields[5]);
                        return busPosition; })
                    .filter(busPositionline -> busPositionline.getVehicleId().equals(String.valueOf(vehicleId)))
                    .forEach(busPositionline -> publisherBusPositions.add(busPositionline));

        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    public String getAddr() {
        return addr;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public List<Topic> getTopics() {
        return topics;
    }

    public void setTopics(List<Topic> topics) {
        this.topics = topics;
    }

    @Override
    public String toString() {
        return "Publisher{" +
                "addr='" + addr + '\'' +
                ", port=" + port +
                '}';
    }
}