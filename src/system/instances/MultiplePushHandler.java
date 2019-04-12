package system.instances;

import system.data.Message;
import system.data.Topic;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Hashtable;
import java.util.Set;

public class MultiplePushHandler extends Thread {

    ObjectInputStream in;
    Hashtable<Topic, Set<Subscriber>> registeredSubscribers;

    public MultiplePushHandler(ObjectInputStream in, Hashtable<Topic, Set<Subscriber>> registeredSubscribers) {
        this.in = in;
        this.registeredSubscribers = registeredSubscribers;
    }

    public void run(){

        try {

            Publisher publisher = (Publisher)in.readObject();
            Message message = (Message)in.readObject();
            System.out.println("Received push message from publisher " + publisher + ". Message: " + message);

            // check if we have registered subscribers on this particular topic
            // and send the message (concurrently) to all of them

            // Hashtable<Topic, Set<Subscriber>> registeredSubscribers = new Hashtable<>();
            if(registeredSubscribers.containsKey(message.getTopic())){
                Set<Subscriber> subscriberSet = registeredSubscribers.get(message.getTopic());
                // TODO prepei na steilw se kathe enan subscriber tis listas to message mou

//                        for(Subscriber subscriber : subscriberSet) {
//                            sendMessage(subscriber, message);
//                        }

                // Αυτό το value  στέλνεται  σε  όλους  τους  subscribers  που  είναι  εγγεγραμμένοι
                // στον συγκεκριμένο broker ταυτόχρονα και ενδιαφέρονται για το ίδιο κλειδί, ώστε να ακολουθήσει το επόμενο κατά σειράδεδομένο.
                // TODO edw xrisimopoiw to parallelStream()
                subscriberSet.parallelStream().forEach(subscriber -> {
                    sendMessage(subscriber, message);
                });

            } else {
                System.out.println("Ignoring message. There are no subscribers for " + message.getTopic() + "topic yet ");
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }

    private void sendMessage(Subscriber subscriber, Message message) {
        Socket requestSocket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;

        try {
            requestSocket = new Socket(subscriber.getAddr(), subscriber.getPort());

            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            int flagRegister = 4; // send flag 4 to subscriber that registered for the message i have in order to signal subscriber that a pull is happening

            try {

                out.writeInt(flagRegister);
                out.flush();

                out.writeObject(message); // send the message to the subscriber
                out.flush();

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
            } catch (Exception e) {
                System.err.println("Subscriber experienced error. This will not affect the runtime of broker ");
            }
        }
    }

}