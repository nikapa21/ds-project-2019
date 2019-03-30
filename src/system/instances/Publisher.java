package system.instances;

import system.data.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class Publisher implements Node{

    List<Topic> topics = new ArrayList<>();


    List<BusLine> publisherBusLines = new ArrayList<>();
    List<RouteCode> publisherRouteCodes = new ArrayList<>();
    List<BusPosition> publisherBusPositions = new ArrayList<>();

    public Publisher(List<String> list) {
        for (String busLine : list) {
            topics.add(new Topic(busLine));
        }
    }
//    void getBrokerList();
//    Broker hashTopic(Topic topic);
//    void push(Topic topic, Value value);
//    void notifyFailure(Broker broker);

    public void init(int port){
        //O
        //publisher node κατά την έναρξη της λειτουργίας του θα πρέπει να γνωρίζει για
        //ποια κλειδιά είναι υπεύθυνος καθώς επίσης και όλη την απαραίτητη
        //πληροφορία για τους brokers. Θα πρέπει να γνωρίζει για κάθε διαθέσιμο
        //broker για ποια κλειδιά είναι υπεύθυνος. Αυτό θα επιτευχθεί κατά τη στιγμή
        //που εκκινείται η λειτουργία του κάθε broker.

        // for example topics responsible for 025, 026, 027

        publisherBusLines = findFromBusLinesFile(topics);
        publisherRouteCodes = findFromRouteCodesFile(publisherBusLines);
        publisherBusPositions = findFromBusPositionsFile(publisherRouteCodes);
    }

    private List<BusLine> findFromBusLinesFile(List<Topic> topics) {

        String busLinesFile = "./Dataset/DS_project_dataset/busLinesNew.txt";
        List<BusLine> publisherBusLines = new ArrayList<>();

        //read file into stream, try-with-resources

        for (Topic topic : topics) {
            try (Stream<String> stream = Files.lines(Paths.get(busLinesFile))) {

                stream.filter(line -> line.contains(topic.getBusLine()))
                        .forEach(line -> {
                            String[] fields = line.split(",");
                            BusLine myBusLine = new BusLine(fields[0], fields[1], fields[2]);
                            publisherBusLines.add(myBusLine);
                        });

            } catch(IOException e){
                e.printStackTrace();
        }

    }
        return publisherBusLines;

    }

    private List<RouteCode> findFromRouteCodesFile(List<BusLine> publisherBusLines) {

        String routeCodesFile = "./Dataset/DS_project_dataset/RouteCodesNew.txt";
        List<RouteCode> publisherRouteCodes = new ArrayList<>();

        //read file into stream, try-with-resources
        for(BusLine busLine:publisherBusLines) {
            try (Stream<String> stream = Files.lines(Paths.get(routeCodesFile))) {


                stream.filter(line -> line.contains(busLine.getLineCode()))
                        .forEach(line -> {
                            String[] fields = line.split(",");
                            RouteCode routeCode = new RouteCode(fields[0], fields[1], fields[2]);
                            publisherRouteCodes.add(routeCode);
                        });


            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return publisherRouteCodes;
    }

    private List<BusPosition> findFromBusPositionsFile(List<RouteCode> publisherRouteCodes) {

        String busPositionsFile = "./Dataset/DS_project_dataset/busPositionsNew.txt";
        List<BusPosition> publisherBusPositions = new ArrayList<>();

        for (RouteCode routeCode : publisherRouteCodes) {
            //read file into stream, try-with-resources
            try (Stream<String> stream = Files.lines(Paths.get(busPositionsFile))) {


                stream.filter(line -> line.contains(routeCode.getRouteCode()))
                        .forEach(line -> {
                            String[] fields = line.split(",");
                            BusPosition busPosition = new BusPosition(fields[0], fields[1], fields[2], Double.parseDouble(fields[3]), Double.parseDouble(fields[4]), fields[5]);
                            publisherBusPositions.add(busPosition);
                        });

            } catch(IOException e){
                e.printStackTrace();
            }
        }

        return publisherBusPositions;
    }

//    void connect();
//    void disconnect();
//    void updateNodes();

}
