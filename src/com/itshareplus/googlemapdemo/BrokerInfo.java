package com.itshareplus.googlemapdemo;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;

public class BrokerInfo implements Serializable {
    private List<Broker> listOfBrokers;
    private Hashtable<Broker, HashSet<Topic>> listOfBrokersResponsibilityLine;

    public BrokerInfo(List<Broker> listOfBrokers, Hashtable<Broker, HashSet<Topic>> listOfBrokersResponsibilityLine) {
        this.listOfBrokers = listOfBrokers;
        this.listOfBrokersResponsibilityLine = listOfBrokersResponsibilityLine;
    }

    public List<Broker> getListOfBrokers() {
        return listOfBrokers;
    }

    public void setListOfBrokers(List<Broker> listOfBrokers) {
        this.listOfBrokers = listOfBrokers;
    }

    public Hashtable<Broker, HashSet<Topic>> getListOfBrokersResponsibilityLine() {
        return listOfBrokersResponsibilityLine;
    }

    public void setListOfBrokersResponsibilityLine(Hashtable<Broker, HashSet<Topic>> listOfBrokersResponsibilityLine) {
        this.listOfBrokersResponsibilityLine = listOfBrokersResponsibilityLine;
    }

    @Override
    public String toString() {
        return "BrokerInfo{" +
                "listOfBrokers=" + listOfBrokers +
                ", listOfBrokersResponsibilityLine=" + listOfBrokersResponsibilityLine +
                '}';
    }
}
