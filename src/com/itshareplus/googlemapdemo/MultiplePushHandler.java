package com.itshareplus.googlemapdemo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Hashtable;
import java.util.Set;

public class MultiplePushHandler extends Thread {

    ObjectInputStream in;
    Broker broker;
    ObjectOutputStream out;
    Hashtable<Topic, Set<Subscriber>> registeredSubscribers;
    Data data;

    Message msg;

    public MultiplePushHandler(ObjectInputStream in, ObjectOutputStream out, Broker broker, Hashtable<Topic, Set<Subscriber>> registeredSubscribers, Message msg) {
        this.in = in;
        this.out = out;
        this.broker  = broker;
        this.registeredSubscribers = registeredSubscribers;
        this.msg = msg;
    }


    public void run() {

        try {

                 Publisher publisher = (Publisher) in.readObject();
                 Data data = (Data) in.readObject();

                 System.out.println("Received push data from publisher " + publisher + ". Data: " + data);

                 // check if we have registered subscribers on this particular topic
                 // and send the data (concurrently) to all of them

                 if (registeredSubscribers.containsKey(data.getTopic()) && registeredSubscribers.get(data.getTopic()).size() != 0) {

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
