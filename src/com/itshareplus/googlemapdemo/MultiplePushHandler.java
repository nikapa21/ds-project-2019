package com.itshareplus.googlemapdemo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Hashtable;
import java.util.Set;

public class MultiplePushHandler extends Thread {

    private int flag;
    Subscriber subscriber;
    String subscriberIp;
    private int flag2;
    ObjectInputStream in;
    Broker broker;
    ObjectOutputStream out;
    ObjectOutputStream out2;
    Hashtable<Topic, Set<Subscriber>> registeredSubscribers;
    Waiter waiter;
    Data data;

    Message msg;

    public MultiplePushHandler(ObjectInputStream in, Hashtable<Topic, Set<Subscriber>> registeredSubscribers) {
        this.in = in;
        this.registeredSubscribers = registeredSubscribers;
    }

    public MultiplePushHandler(ObjectInputStream in, Broker broker, Hashtable<Topic, Set<Subscriber>> registeredSubscribers) {
        this.in = in;
        this.broker  = broker;
        this.registeredSubscribers = registeredSubscribers;
    }

    public MultiplePushHandler(ObjectInputStream in, ObjectOutputStream out, Broker broker, Hashtable<Topic, Set<Subscriber>> registeredSubscribers) {
        this.in = in;
        this.out = out;
        this.broker  = broker;
        this.registeredSubscribers = registeredSubscribers;
    }

    public MultiplePushHandler(ObjectInputStream in, ObjectOutputStream out, Broker broker, Hashtable<Topic, Set<Subscriber>> registeredSubscribers, int flag2) {
        this.in = in;
        this.out = out;
        this.broker  = broker;
        this.registeredSubscribers = registeredSubscribers;
        this.flag2 = flag2;
    }

    public MultiplePushHandler(ObjectInputStream in, ObjectOutputStream out, Broker broker, Hashtable<Topic, Set<Subscriber>> registeredSubscribers, int flag2, Subscriber subscriber) {
        this.in = in;
        this.out = out;
        this.broker  = broker;
        this.registeredSubscribers = registeredSubscribers;
        this.flag2 = flag2;
        this.subscriber = subscriber;
    }

    public MultiplePushHandler(ObjectInputStream in, ObjectOutputStream out, Broker broker, Hashtable<Topic, Set<Subscriber>> registeredSubscribers, int flag2, String subscriberIp, Waiter waiter) {
        this.in = in;
        this.out = out;
        this.broker  = broker;
        this.registeredSubscribers = registeredSubscribers;
        this.flag2 = flag2;
        this.subscriberIp = subscriberIp;
        this.waiter = waiter;
    }

    public MultiplePushHandler(ObjectInputStream in, ObjectOutputStream out, Broker broker, Hashtable<Topic, Set<Subscriber>> registeredSubscribers, Waiter waiter) {
        this.in = in;
        this.out = out;
        this.broker = broker;
        this.registeredSubscribers = registeredSubscribers;
        this.waiter = waiter;
    }

    public MultiplePushHandler(ObjectInputStream in, ObjectOutputStream out, Broker broker, Hashtable<Topic, Set<Subscriber>> registeredSubscribers, Message msg) {
        this.in = in;
        this.out = out;
        this.broker  = broker;
        this.registeredSubscribers = registeredSubscribers;
        this.msg = msg;
    }

//    public MultiplePushHandler(ObjectInputStream in, Hashtable<Topic, Set<Subscriber>> registeredSubscribers, ObjectOutputStream out) {
//        this.in = in;
//        this.registeredSubscribers = registeredSubscribers;
//        this.out = out;
//    }
//
//    public MultiplePushHandler(ObjectInputStream in, Hashtable<Topic, Set<Subscriber>> registeredSubscribers, ObjectOutputStream out, Waiter waiter) {
//        this.in = in;
//        this.registeredSubscribers = registeredSubscribers;
//        this.out = out;
//        this.waiter = waiter;
//    }
//
//    public MultiplePushHandler(ObjectInputStream in, int flag2, Hashtable<Topic, Set<Subscriber>> registeredSubscribers, ObjectOutputStream out, Waiter waiter) {
//        this.in = in;
//        this.flag2 = flag2;
//        this.registeredSubscribers = registeredSubscribers;
//        this.out = out;
//        this.waiter = waiter;
//    }
//
//    public MultiplePushHandler(ObjectInputStream in, int flag2, Hashtable<Topic, Set<Subscriber>> registeredSubscribers, ObjectOutputStream out) {
//        this.in = in;
//        this.flag2 = flag2;
//        this.registeredSubscribers = registeredSubscribers;
//        this.out = out;
//    }

    public void run() {

        try {

                 Publisher publisher = (Publisher) in.readObject();
                 Data data = (Data) in.readObject();

                 // possible TODO
                 // For every data that broker receives from publisher
                 // the broker knows that publisher is still alive and functioning
                 // and will update a data structure with the most recent heartbeats
                 // kai meta tha ftiaksw ena allo thread kai tha tsekarei poios apo tous registered den exei recent (5 seconds e.g)heartbeat
                 // kai tha stelnei to antistoixo mhnuma ston subscriber

                 System.out.println("Received push data from publisher " + publisher + ". Data: " + data);


                 // check if we have registered subscribers on this particular topic
                 // and send the data (concurrently) to all of them

                 // Hashtable<Topic, Set<Subscriber>> registeredSubscribers = new Hashtable<>();

                 if (registeredSubscribers.containsKey(data.getTopic())) {
                     Set<Subscriber> subscriberSet = registeredSubscribers.get(data.getTopic());

//                     int flag = 4;
////=
//
//                         System.out.println("GOT IN HEREEEEE");
//                         subscriberSet.parallelStream().forEach(subscriber1 -> {
//                             sendMessage(subscriber1, data, broker, flag, in, out);
//
//
//                         });

                     Notifier notifier = new Notifier(msg);
                     msg.setData(data);
                     msg.setMsg("Notification sent ");
                     new Thread(notifier).start();

                 } else {
                     System.out.println("Ignoring data. There are no subscribers for " + data.getTopic() + "topic yet ");
                 }


        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


    private void sendMessage(Subscriber subscriber, Data data, Broker broker, int flag, ObjectInputStream in, ObjectOutputStream out) {

        try {

            Socket requestSocket = null;

            requestSocket  = new Socket("192.168.1.4", broker.getPort());

            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            out.writeInt(flag);
            out.flush();

            if (flag == 4) { // send the requested file back to user

                System.out.println("Sending the requested data from Thread to Broker and back to user " );
                out.writeObject(data);
                out.flush();

            }

        } catch (UnknownHostException unknownHost) {
            System.err.println("You are trying to connect to an unknown host!");
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            try {
                in.close();
                out.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            } catch (Exception e) {
                System.err.println("Subscriber experienced error. This will not affect the runtime of broker ");
            }
        }
    }

    public Data callData() throws InterruptedException {

        Thread.sleep(2000);
        return data;
    }
}
