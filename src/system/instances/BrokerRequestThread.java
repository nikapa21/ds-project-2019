package system.instances;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class BrokerRequestThread {


    public static void main(String[] args) {
        new BrokerRequestThread().startClient();
    }

    public void startClient() {
        Socket requestSocket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;
        String message;
        try {
            requestSocket = new Socket(InetAddress.getByName("192.168.1.2"), 7000);

            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            try {
                message = (String) in.readObject();
                System.out.println("Server>" + message);

                out.writeObject("Hi!");
                out.flush();

                out.writeObject("1237248914327929348L");
                out.flush();

                out.writeObject("bye");
                out.flush();
            } catch(ClassNotFoundException classNot){
                System.err.println("data received in unknown format");
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
}