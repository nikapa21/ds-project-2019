package system.instances;

import com.sun.xml.internal.bind.v2.TODO;
import system.data.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MyPublisher implements Publisher{

    List<Topic> topics = new ArrayList<>();

    List<BusLine> publisherBusLines = new ArrayList<>();
    List<RouteCode> publisherRouteCodes = new ArrayList<>();
    List<BusPosition> publisherBusPositions = new ArrayList<>();

    /***** NODE COMMON METHODS *****/

    @Override
    public void init(int vehicleId){ //10374
        // vres ola ta kleidia gia ta opoia eisai upeuthinos
        initiateTopicList(vehicleId);

        // mathe oli tin aparaititi pliroforia gia tous brokers
        getBrokerList();

        // se auto to simeio apothikeusame sti mnimi ( se mia metavliti ) tin aparaititi pliroforia
        // opws to exw skeftei auto tha apothikeutei sti static final brokers tis Node! ara se kathe init kathe publisher tha ginetai ksana k ksana populate i idia lista
        // giati oloi oi publishers tha exoun to idio txt. O monos tropos na to controllaroume auto einai me ena if (size != 0).

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

    /***** PUBLISHER METHODS *****/

    @Override
    public List<Broker> getBrokers(){
        return null;
    }

    // TODO Open the file, parse the file, and populate Node's final static list brokers
    @Override
    public void getBrokerList() {

    }

    @Override
    public Broker hashTopic(Topic topic) {
        return null;
    }

    @Override
    public void push(Topic topic, Value value) {

    }

    @Override
    public void notifyFailure(Broker broker) {

    }

    // TODO fix potential filter bug
    private void initiateTopicList(int vehicleId) {

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

            stream.map(line -> {
                        String[] fields = line.split(",");
                        BusPosition busPosition = new BusPosition(fields[0], fields[1], fields[2], Double.parseDouble(fields[3]), Double.parseDouble(fields[4]), fields[5]);
                        return busPosition; })
                    .filter(busPositionline -> busPositionline.getVehicleId().equals(String.valueOf(vehicleId)))
                    .forEach(busPositionline -> {
                        publisherBusPositions.add(busPositionline);
                    });

//            stream.filter(line -> line
//                    .contains(String.valueOf(vehicleId)+ ","))
//                    .forEach(line -> {
//                        String[] fields = line.split(",");
//                        BusPosition busPosition = new BusPosition(fields[0], fields[1], fields[2], Double.parseDouble(fields[3]), Double.parseDouble(fields[4]), fields[5]);
//                        publisherBusPositions.add(busPosition);
//                    });

        } catch(IOException e){
            e.printStackTrace();
        }

        return publisherBusPositions;
    }

}
