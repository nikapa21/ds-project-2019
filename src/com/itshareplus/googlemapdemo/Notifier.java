package com.itshareplus.googlemapdemo;

public class Notifier implements Runnable {

        private Message msg;

    public Notifier(Message data) {
            this.msg = data;
        }

        @Override
        public void run() {
            String name = Thread.currentThread().getName();
            System.out.println(name+" started");

                //Thread.sleep(1000);
                synchronized (msg) {
                    //System.out.println("Inside notify synchronized ");
                    msg.notify();
                    // requestedFile.notifyAll();
                }


        }

}