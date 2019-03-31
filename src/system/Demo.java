package system;

import system.data.BusLine;
import system.instances.Broker;
import system.instances.Publisher;
import system.instances.Subscriber;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class Demo {
    public static void main(String[] args) {
        Broker broker1 = new Broker();
        Broker broker2 = new Broker();

        List<String> busLinesList = new ArrayList<>();
        busLinesList.add("025");

        //Publisher publisher = new Publisher(busLinesList);
        Set<String> allVehicles = findAllVehicleIds();

        for(String vehicleId : allVehicles){
            Publisher publisher= new Publisher();
            System.out.println("Vehicle " + vehicleId);
            publisher.init(Integer.parseInt(vehicleId.trim())); // .trim giati eskage se kapoio lathos(?) tou dataset
        }
        System.out.println("Found " + allVehicles.size() + " vehicles");

        Subscriber subscriber = new Subscriber();

        // start broker 1

    }

    public static Set<String> findAllVehicleIds(){
        String busPositionsFile = "./Dataset/DS_project_dataset/busPositionsNew.txt";
        Set<String> distinctVehicleIds = new HashSet<>();

        try (Stream<String> stream = Files.lines(Paths.get(busPositionsFile))) {

            stream.forEach(line -> {
                        String[] fields = line.split(",");
                        String vehicle = fields[2];
                        distinctVehicleIds.add(vehicle);
                    });

        } catch(IOException e){
            e.printStackTrace();
        }

        return distinctVehicleIds;
    }

}
