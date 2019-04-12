package system.instances;

import system.data.Message;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class SubscriberListeningThread extends Thread {

    String addr;
    int port;

    public SubscriberListeningThread(int port) {
        this.port = port;
    }
    public void run() {
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
                    System.out.println("A message is coming " + message);
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
}
