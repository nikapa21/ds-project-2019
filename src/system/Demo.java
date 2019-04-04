package system;

import system.instances.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class Demo {
    public static void main(String[] args) {

//        Broker broker1 = new Broker();
//        Broker broker2 = new Broker();

        //MyPublisher publisher = new MyPublisher(busLinesList);
        Set<String> allVehicles = findAllVehicleIds();

        for(String vehicleId : allVehicles){
            MyPublisher myPublisher = new MyPublisher();
            System.out.println("Vehicle " + vehicleId);
            myPublisher.init(Integer.parseInt(vehicleId.trim())); // .trim giati eskage se kapoio lathos(?) tou dataset
        }


        System.out.println("Found " + allVehicles.size() + " vehicles");


        //Subscriber subscriber = new Subscriber();

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