package system;

import system.data.BusLine;
import system.instances.Broker;
import system.instances.Publisher;
import system.instances.Subscriber;

public class Demo {
    public static void main(String[] args) {
        Broker broker1 = new Broker();
        Broker broker2 = new Broker();

        Publisher publisher = new Publisher();
        Subscriber subscriber = new Subscriber();

        // start broker 1

    }
}
