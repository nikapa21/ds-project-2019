package system.instances;

import com.sun.xml.internal.bind.v2.TODO;
import system.data.*;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MyPublisher implements Publisher {

    List<Topic> topics = new ArrayList<>();
    List<Value> publisherValues = new ArrayList<>();

    List<BusLine> publisherBusLines = new ArrayList<>();
    List<RouteCode> publisherRouteCodes = new ArrayList<>();
    List<BusPosition> publisherBusPositions = new ArrayList<>();

    public static void main(String[] args) {

    }
    /***** NODE COMMON METHODS *****/

    @Override
    public void init(int vehicleId) { //10374

        // vres ola ta kleidia gia ta opoia eisai upeuthinos
        initiateTopicAndValueList(vehicleId);

        // mathe oli tin aparaititi pliroforia gia tous brokers
        if(Node.brokers.size() == 0) {
            getBrokerList();
        }

        // se auto to simeio apothikeusa sti mnimi ( se mia metavliti ) tin aparaititi pliroforia
        // opws to exw skeftei auto tha apothikeutei sti static final brokers tis Node! ara se kathe init kathe publisher tha ginetai ksana k ksana populate i idia lista
        // giati oloi oi publishers tha exoun to idio txt. O monos tropos na to controllarw auto einai me ena if (size != 0).

        // kane register stous antistoixous brokers (connect)
        connect();

        // ksekina to stream (push)
        for(Topic topic : topics) {
            for(Value value : publisherValues) {
                if(topic.getBusLine().equals(value.getBuslineId()))
                    push(topic, value);
            }
        }
    }

    @Override
    public void connect() {

        // kane connect stous antistoixous brokers gia ta antistoixa topics

        for(Topic topic : topics) {
            Broker broker = hashTopic(topic); // prepei na to kanw gia kathe topic
            broker.acceptConnection(this); // this diladi myPublisher.
            // to mono pou tha kanei einai oti stin acceptConnection tou Broker tha ginei add sti lista tou registeredPublishers
        }

    }

    @Override
    public void disconnect() {

    }

    @Override
    public void updateNodes() {

    }

    /***** PUBLISHER METHODS *****/

    @Override
    public List<Broker> getBrokers(){
        return null;
    }

    @Override
    public void getBrokerList() {

        String brokersFile = "./Dataset/DS_project_dataset/BrokersList.txt";

        // read file into stream, try-with-resources
        try (Stream<String> stream = Files.lines(Paths.get(brokersFile))) {

            stream.map(line -> {
                String[] fields = line.split(",");
                MyBroker broker = new MyBroker(fields[0], Integer.parseInt(fields[1]));
                return broker; })
                    .forEach(line -> Node.brokers.add(line));

        } catch(IOException e) {
            e.printStackTrace();
        }

    }

    @Override
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

        for(Broker broker : brokers) {
            String brokerHash = null;// hash the name of file with sha1
            try {
                MyBroker mybroker = (MyBroker)broker;
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

            System.out.println(brokerHashesList.get(i));
            if((Math.abs(publisherModKey - brokerHashesList.get(i))) < minDistance){
                minDistance = Math.abs(publisherModKey - brokerHashesList.get((i)));
                nodeId = i;
            }
        }

        Broker broker = brokers.get(nodeId);

        System.out.println("MinDistance is " + minDistance + " and broker node that should be chosen is " + broker + " with id " + nodeId);
        System.out.println();

        return broker;
    }

    @Override
    public void push(Topic topic, Value value) {

        // vres ton broker
        Broker broker = hashTopic(topic);
        // steile ta dedomena
        // kati tou eidous kaneis ena thread pou ksekinaei ena socket se auto to ip kai auto to port tou broker kai kanei push to value

    }

    @Override
    public void notifyFailure(Broker broker) {

    }

    private void initiateTopicAndValueList(int vehicleId) {

        // Me vasi to vehicle id vres ola ta bus position objects apo to arxeio me ta bus positions
        publisherBusPositions = findFromBusPositionsFile(vehicleId);

        // Vres ola ta distinct route codes apo ta parapanw bus position objects
        List<String> distinctRouteCodes = publisherBusPositions.stream().map(BusPosition::getRouteCode).distinct().collect(Collectors.toList());

        // Apo ta distinct route codes Strings vres ola ta route code objects apo to arxeio me ta route codes
        publisherRouteCodes = findFromRouteCodesFile(distinctRouteCodes);

        // Vres ola ta distinct line codes apo ta parapanw route code objects
        List<String> distinctLineCodes = publisherRouteCodes.stream().map(RouteCode::getLineCode).distinct().collect(Collectors.toList());

        // Apo ta distinct bus lines Strings vres ola ta bus line objects apo to arxeio me ta bus lines
        publisherBusLines = findFromBusLinesFile(distinctLineCodes);

        // TODO edw prepei na gemisw ti publisherValue list kanontas iterate stis 3 listes pou ftiaksame apo panw

        // publisherValues

        System.out.print("Vehicle with id " + vehicleId + " is responsible for the following lines/topics: ");
        for(BusLine busLine: publisherBusLines) {
            topics.add(new Topic(busLine.getLineId()));
            System.out.print(busLine.getLineId() + " ");
        }
        System.out.println();
    }

    private List<BusLine> findFromBusLinesFile(List<String> busLines) {

        String busLinesFile = "./Dataset/DS_project_dataset/busLinesNew.txt";
        List<BusLine> publisherBusLines = new ArrayList<>();

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

        return publisherBusLines;

    }

    private List<RouteCode> findFromRouteCodesFile(List<String> routeCodes) {

        String routeCodesFile = "./Dataset/DS_project_dataset/RouteCodesNew.txt";
        List<RouteCode> publisherRouteCodes = new ArrayList<>();

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

        return publisherRouteCodes;
    }

    private List<BusPosition> findFromBusPositionsFile(int vehicleId) {

        String busPositionsFile = "./Dataset/DS_project_dataset/busPositionsNew.txt";
        List<BusPosition> publisherBusPositions = new ArrayList<>();

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

        return publisherBusPositions;
    }

}