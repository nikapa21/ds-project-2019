package com.itshareplus.googlemapdemo;

public class Notifier implements Runnable {

    private Message msg;

    public Notifier(Message data) {
            this.msg = data;
        }

        @Override
        public void run() {
                synchronized (msg) {
                    //System.out.println("Inside notify synchronized ");
                    msg.notify();
                }
        }
}