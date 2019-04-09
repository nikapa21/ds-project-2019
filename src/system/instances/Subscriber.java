package system.instances;

import sun.awt.SubRegionShowable;
import system.data.Message;
import system.data.Topic;
import system.data.Value;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Hashtable;
import java.util.List;

public class Subscriber implements Serializable {

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

        Subscriber subscriber = new Subscriber();
        Topic topic = new Topic(args[0]);

        // kane register ton subscriber
        subscriber.doTheRegister(topic);

        // afou pires oli tin aparaititi pliroforia apo tous brokers kai gia poia kleidia einai upeuthinoi
        // zita apo sugkekrimeno broker to topic sou gia na sou epistrepsei to value kai na to optikopoihseis


    }

    private void doTheRegister(Topic topic) {
        Socket requestSocket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;

        try {
            requestSocket = new Socket("127.0.0.1", 7000);

            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            int flagRegister = 2; // send flag 2 to broker 7000 in order to register subscriber

            try {

                out.writeInt(flagRegister);
                out.flush();

                out.writeObject(this); // send the subscriber himself (this) to be registered
                out.flush();

                // an o subscriber dwsei moufa topic???
                out.writeObject(topic);
                out.flush();
                // perimenw na mathw poioi einai oi upoloipoi brokers kai gia poia kleidia einai upeuthinoi
                // diladi perimenw ena antikeimeno Info tis morfis {ListOfBrokers, <BrokerId, ResponsibilityLine>}

                brokerInfo = (BrokerInfo)in.readObject();
                System.out.println(brokerInfo);



//                Hashtable listOfBrokersResponsibilityLine = (Hashtable)in.readObject();
//                System.out.println(listOfBrokersResponsibilityLine);


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

//    public void init() {
//
//        ServerSocket providerSocket = null;
//        Socket connection = null;
//
//        try {
//            providerSocket = new ServerSocket(8000);
//
//            while (true) {
//                connection = providerSocket.accept();
//
//                ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
//                ObjectInputStream in = new ObjectInputStream(connection.getInputStream());
//                int flag;
//
//                flag = in.readInt();
//
//                if (flag == 0) {
//
//                    // TODO register publisher
//                    Topic topic = (Topic)in.readObject();
//                    System.out.println("Broker " + this + " accepted a registration from publisher " + publisher + " for the topic " + topic);
//                }
//
//                else if (flag == 1) {
//                    Publisher publisher = (Publisher)in.readObject();
//                    Message message = (Message)in.readObject();
//                    System.out.println("Received push message from publisher " + publisher + ". Message: " + message);
//                }
//
//                else if (flag == 2) {
//                    Subscriber subscriber = (Subscriber)in.readObject();
//                    Topic topic = (Topic)in.readObject();
//                    System.out.println("Broker " + this + " accepted a registration from subsriber " + subscriber + " for the topic " + topic);
//                }
//
//                in.close();
//                out.close();
//                connection.close();
//            }
//        } catch (IOException ioException) {
//            ioException.printStackTrace();
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            try {
//                providerSocket.close();
//            } catch (IOException ioException) {
//                ioException.printStackTrace();
//            }
//        }
//
//    }

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

    @Override
    public String toString() {
        return "Subscriber{" +
                "addr='" + addr + '\'' +
                ", port=" + port +
                '}';
    }
}
