//package com.itshareplus.googlemapdemo;
//
//import java.io.IOException;
//import java.io.ObjectInputStream;
//import java.io.ObjectOutputStream;
//import java.util.Set;
//
//public class MultiplePullHandler extends Thread{
//
//    Subscriber subscriber;
//    Data data;
//    ObjectInputStream in;
//    ObjectOutputStream out;
//    Waiter waiter;
//    Set<Subscriber> subscriberSet;
//
//    public MultiplePullHandler(Subscriber subscriber, Data data) {
//        this.subscriber = subscriber;
//        this.data = data;
//    }
//
//    public MultiplePullHandler(ObjectInputStream in, ObjectOutputStream out, Subscriber subscriber, Waiter waiter) {
//        this.in = in;
//        this.out = out;
//        this.subscriber = subscriber;
//        this.waiter = waiter;
//    }
//
//    public MultiplePullHandler(Data data, Set<Subscriber> subscriberSet) {
//        this.data = data;
//        this.subscriberSet = subscriberSet;
//    }
//
//    public void run() {
//
//        try {
//
////            Subscriber subscriber = (Subscriber) in.readObject();

//
////            out.writeObject(data);
////            out.flush();
//
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//    }
//}
