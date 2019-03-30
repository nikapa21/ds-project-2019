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
import java.util.List;
import java.util.stream.Stream;

public class Demo {
    public static void main(String[] args) {
        Broker broker1 = new Broker();
        Broker broker2 = new Broker();

        List<String> busLinesList = new ArrayList<>();
        busLinesList.add("025");

        Publisher publisher = new Publisher(busLinesList);
        publisher.init(7777);

        Subscriber subscriber = new Subscriber();

        // start broker 1

    }
}
