package com.itshareplus.googlemapdemo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

public class Subscriber implements Serializable {

    private static final long serialVersionUID = 1L;

    String addr;
    int port;
    BrokerInfo brokerInfo;


    public Subscriber(String addr, int port) {
        this.addr = addr;
        this.port = port;
    }

    public Subscriber() {

    }

    public static void main(String[] args) {

        Subscriber subscriber = new Subscriber("192.168.1.4", Integer.parseInt(args[1]));
        Topic topic = new Topic(args[0]);

        // kane preRegister ton subscriber
        subscriber.doThePreRegister();

        // afou pires oli tin aparaititi pliroforia apo tous brokers kai gia poia kleidia einai upeuthinoi
        // zita apo sugkekrimeno broker to topic sou gia na sou epistrepsei to value kai na to optikopoihseis
        Broker broker = subscriber.findMyBrokerForMyTopic(topic);

        // subscriber do the register(broker, topic)
        subscriber.doTheRegister(broker, topic);

        SubscriberListeningThread slt = new SubscriberListeningThread(subscriber.port); // Port h .getPort()?
        slt.start();

        // subscriber.openServer();

        // meta to register, o subscriber tha kanei ena openserver gia na perimenei ta data apo ton broker.

    }

    public void openServer() {
        ServerSocket providerSocket = null;
        Socket connection = null;

        try {
            providerSocket = new ServerSocket(port);

            while (true) {
                connection = providerSocket.accept();

                ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(connection.getInputStream());

                int flag;

                flag = in.readInt();

                if(flag == 4) {
                    Message message = (Message)in.readObject();
                    System.out.println("Timestamp: " + System.currentTimeMillis() + "A message is coming " + message); // vazoume ena timestamp gia na fainetai oti ginetai parallila to consuming anamesa stous subscribers
                }

                in.close();
                out.close();
                connection.close();
            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                providerSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    private void doTheRegister(Broker broker, Topic topic) {
        Socket requestSocket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;

        try {
            requestSocket = new Socket(broker.getIpAddress(), broker.getPort());

            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            int flagRegister = 3; // send flag 3 to responsible broker for topic in order to register subscriber and pull

            try {

                out.writeInt(flagRegister);
                out.flush();

                out.writeObject(this); // send the subscriber himself (this) to be registered
                out.flush();

                out.writeObject(topic);
                out.flush();

                // de tha exoume while in.read. Tha kleinei to connection kai tha kanoume expect se ena listening thread as poume
                // oti tou stelnoume tou broker oti egw eimai se auto to ip, port (afou tou exw steilw to this), kai opote erthoun ta
                // data ston broker na ta kanei push sto tade port me ena sugkekrimeno flag. egw tha perimenw na mou rthei ena tuple kai tha to kanw system.out.

            } catch(Exception classNot){
                System.err.println("data received in unknown format");
                classNot.printStackTrace();
            }
        } catch (UnknownHostException unknownHost) {
            System.err.println("You are trying to connect to an unknown host!");
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            try {
                in.close();
                out.close();
                requestSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    private Broker findMyBrokerForMyTopic(Topic topic) {
        Broker myBroker = null;

        for(Broker broker : brokerInfo.getListOfBrokersResponsibilityLine().keySet()) {
            HashSet<Topic> mySet = brokerInfo.getListOfBrokersResponsibilityLine().get(broker);
            if (mySet.contains(topic)) {
                // an to mySet exei to topic krata to key
                myBroker = broker;
                break;
            }
        }
        return myBroker;
    }

    private void doThePreRegister() {
        Socket requestSocket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;

        try {
            requestSocket = new Socket("192.168.1.4", 7000);

            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            int flagRegister = 2; // send flag 2 to broker 7000 in order to preRegister subscriber and receive all info about brokers and responsibilities

            try {

                out.writeInt(flagRegister);
                out.flush();

                // perimenw na mathw poioi einai oi upoloipoi brokers kai gia poia kleidia einai upeuthinoi
                // diladi perimenw ena antikeimeno Info tis morfis {ListOfBrokers, <BrokerId, ResponsibilityLine>}

                brokerInfo = (BrokerInfo)in.readObject();
                System.out.println("Received from broker brokerinfo upon preregister: " + brokerInfo);


            } catch(Exception classNot){
                System.err.println("data received in unknown format");
                classNot.printStackTrace();
            }
        } catch (UnknownHostException unknownHost) {
            System.err.println("You are trying to connect to an unknown host!");
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            try {
                in.close();
                out.close();
                requestSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Subscriber that = (Subscriber) o;
        return port == that.port &&
                addr.equals(that.addr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(addr, port);
    }

    public void connect() {

    }

    public void disconnect() {

    }

    public void updateNodes() {

    }

    public List<Broker> getBrokers() {
        return null;
    }

    public void register(Broker broker, Topic topic) {

    }

    public void disconnect(Broker broker, Topic topic) {

    }

    public void visualiseData(Topic topic, Value value) {

    }

    public String getAddr() {
        return addr;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public BrokerInfo getBrokerInfo() {
        return brokerInfo;
    }

    public void setBrokerInfo(BrokerInfo brokerInfo) {
        this.brokerInfo = brokerInfo;
    }

    @Override
    public String toString() {
        return "Subscriber{" +
                "addr='" + addr + '\'' +
                ", port=" + port +
                '}';
    }
}
