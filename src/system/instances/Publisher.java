package system.instances;

import system.data.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Publisher implements Node{

    List<Topic> topics = new ArrayList<>();

    List<BusLine> publisherBusLines = new ArrayList<>();
    List<RouteCode> publisherRouteCodes = new ArrayList<>();
    List<BusPosition> publisherBusPositions = new ArrayList<>();
//
//    public Publisher(List<String> list) {
//        for (String busLine : list) {
//            topics.add(new Topic(busLine));
//        }
//    }
//    void getBrokerList();
//    Broker hashTopic(Topic topic);
//    void push(Topic topic, Value value);
//    void notifyFailure(Broker broker);

    public void init(int vehicleId){ //10374
        //O
        //publisher node κατά την έναρξη της λειτουργίας του θα πρέπει να γνωρίζει για
        //ποια κλειδιά είναι υπεύθυνος καθώς επίσης και όλη την απαραίτητη
        //πληροφορία για τους brokers. Θα πρέπει να γνωρίζει για κάθε διαθέσιμο
        //broker για ποια κλειδιά είναι υπεύθυνος. Αυτό θα επιτευχθεί κατά τη στιγμή
        //που εκκινείται η λειτουργία του κάθε broker.

        // for example topics responsible for 025, 026, 027

        //personList.stream()
        //  .filter(distinctByKey(p -> p.getName()))
        //  .collect(Collectors.toList());

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

        System.out.print("Vehicle with id " + vehicleId + " is responsible for the following lines/topics: ");
        for(BusLine busLine: publisherBusLines){
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

                stream.filter(line -> line.contains(busLine + ","))
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

    private List<RouteCode> findFromRouteCodesFile(List<String> routeCodes) {

        String routeCodesFile = "./Dataset/DS_project_dataset/RouteCodesNew.txt";
        List<RouteCode> publisherRouteCodes = new ArrayList<>();

        //read file into stream, try-with-resources
        for(String routeCode : routeCodes) {
            try (Stream<String> stream = Files.lines(Paths.get(routeCodesFile))) {

                stream.filter(line -> line.contains(routeCode + ","))
                        .forEach(line -> {
                            String[] fields = line.split(",");
                            RouteCode routeCodeObject = new RouteCode(fields[1], fields[0], fields[3]);
                            publisherRouteCodes.add(routeCodeObject);
                        });

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return publisherRouteCodes;
    }

    private List<BusPosition> findFromBusPositionsFile(int vehicleId) {

        String busPositionsFile = "./Dataset/DS_project_dataset/busPositionsNew.txt";
        List<BusPosition> publisherBusPositions = new ArrayList<>();

        //read file into stream, try-with-resources
        try (Stream<String> stream = Files.lines(Paths.get(busPositionsFile))) {

            stream.filter(line -> line.contains(String.valueOf(vehicleId)+ ","))
                    .forEach(line -> {
                        String[] fields = line.split(",");
                        BusPosition busPosition = new BusPosition(fields[0], fields[1], fields[2], Double.parseDouble(fields[3]), Double.parseDouble(fields[4]), fields[5]);
                        publisherBusPositions.add(busPosition);
                    });

        } catch(IOException e){
            e.printStackTrace();
        }

        return publisherBusPositions;
    }

//    void connect();
//    void disconnect();
//    void updateNodes();

}
