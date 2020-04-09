package com.itshareplus.googlemapdemo;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Hashtable;
import java.util.Set;

public class Waiter implements Runnable{

    private Message msg;
    private ObjectOutputStream out;
    private Hashtable<ArtistName, Set<Subscriber>> registeredSubscribers;
    private Subscriber subscriber;


    public Waiter(Message m, ObjectOutputStream out, Hashtable<ArtistName, Set<Subscriber>> registeredSubscribers, Subscriber subscriber){
        this.msg = m;
        this.out = out;
        this.registeredSubscribers = registeredSubscribers;
        this.subscriber = subscriber;
    }

    @Override
    public void run() {
        boolean socketIsOpen = true;

        while(socketIsOpen) {
            synchronized (msg) {
                try {
                    msg.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                try {
                    System.out.println("Sending data to subscriber!");
                    out.writeObject(msg.getData());
                    out.flush();
                } catch (IOException e) {
                    System.out.println("The subscriber seems to have bailed out! The socket is closed. Removing the subscriber ");
                    registeredSubscribers.get(msg.getData().getTopic()).remove(subscriber);

                    socketIsOpen = false;
                }
            }
        }
    }
}

