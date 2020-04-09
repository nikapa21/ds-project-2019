package com.itshareplus.googlemapdemo;

import com.itshareplus.googlemapdemo.models.MusicFile;
import com.itshareplus.googlemapdemo.models.Value;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.mp3.Mp3Parser;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class Publisher extends Thread implements Serializable {

    String addr;
    int port;
    BrokerInfo brokerInfo;
    private static final String BASE_DIRECTORY = "./dataset1/";

    List<ArtistName> artistNames = new ArrayList<>();

    List<Value> publisherValues = new ArrayList<>();

    List<Broker> brokersCluster = new ArrayList<>();

    public Publisher(String addr, int port) {

        this.addr = addr;
        this.port = port;
    }

    public Publisher() {

    }

    public static void main(String[] args) {

        Publisher publisher = new Publisher("192.168.1.7", Integer.parseInt(args[2]));

        // O publisher node κατά την έναρξη της λειτουργίας του θα πρέπει να γνωρίζει για
        //ποια κλειδιά  είναι  υπεύθυνος  καθώς  επίσης  και  όλη  την  απαραίτητη πληροφορία  για  τους  brokers.

        // vres ola ta kleidia gia ta opoia eisai upeuthinos. Diavase apo ta arxeia to vehicleId kai ta topics pou prokuptoun analogws

        String genre = args[0];
        String genreDirectory = BASE_DIRECTORY + genre;

        List<String> allSongsFromGenre = publisher.getAllSongsFromGenre(genreDirectory);

        publisher.init(allSongsFromGenre, Integer.parseInt(args[1]));

        // mathe tin aparaititi pliroforia gia tous brokers diladi vres apo to arxeio olous tous brokers

        publisher.getBrokerList();

        // kai sugxronisou me enan apo autous wste na sou pei gia poia topics einai o kathe broker upeuthinos.
        // o publisher tha parei oli tin pliroforia gia to poios einai upeuthinos. xwris na kanei hashTopic.
        publisher.fetchAllTheBrokerInfo();

        for (ArtistName topic : publisher.artistNames) {
            Broker broker = publisher.findMyBrokerForMyTopic(topic); // prepei na to kanw gia kathe topic
            publisher.doTheRegister(broker, topic);
        }

        // kanoume push ola ta values pou exoume (idanika xrisimopoiwntas sleep)

        for (Value value : publisher.publisherValues) {
            ArtistName topic = new ArtistName(value.getMusicFile().getArtistName());
            Broker broker = publisher.hashTopic(topic);
            Data data = new Data(topic, value);
            publisher.pushTheMessageToBroker(broker, data);
        }
    }

    private Broker findMyBrokerForMyTopic(ArtistName topic) {

        Broker myBroker = null;

        for (Broker broker : brokerInfo.getListOfBrokersResponsibilityLine().keySet()) {
            HashSet<ArtistName> mySet = brokerInfo.getListOfBrokersResponsibilityLine().get(broker);
            if (mySet.contains(topic)) {
                // an to mySet exei to topic krata to key
                myBroker = broker;
                break;
            }
        }
        return myBroker;
    }

    private void fetchAllTheBrokerInfo() {

        Socket requestSocket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;

        try {
            requestSocket = new Socket("192.168.1.7", 7000);

            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            int flagRegister = 2; // send flag 2 to broker 7000 in order to fetch all info about brokers and responsibilities

            try {

                out.writeInt(flagRegister);
                out.flush();

                // perimenw na mathw poioi einai oi upoloipoi brokers kai gia poia kleidia einai upeuthinoi
                // diladi perimenw ena antikeimeno Info tis morfis {ListOfBrokers, <BrokerId, ResponsibilityLine>}
                String greetingMessage = (String) in.readObject();
                System.out.println(greetingMessage);

                brokerInfo = (BrokerInfo) in.readObject();
                System.out.println("Received from broker brokerinfo upon preregister: " + brokerInfo);
            } catch (Exception classNot) {
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

    private void doTheRegister(Broker broker, ArtistName topic) {

        Socket requestSocket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;

        try {
            requestSocket = new Socket(InetAddress.getByName(broker.getIpAddress()), broker.getPort());

            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            int flagRegister = 0; // send flag 0 to register publisher

            try {

                out.writeInt(flagRegister);
                out.flush();

                out.writeObject(this); // send the publisher himself (this) to be registered
                out.flush();

                out.writeObject(topic);
                out.flush();
            } catch (Exception classNot) {
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

    public void pushTheMessageToBroker(Broker broker, Data data) {

        Socket requestSocket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;

        try {
            requestSocket = new Socket(InetAddress.getByName(broker.getIpAddress()), broker.getPort());

            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            int flagPush = 1; // send push data

            try {

                out.writeInt(flagPush);
                out.flush();

                out.writeObject(this);
                out.flush();

                out.writeObject(data);
                out.flush();
            } catch (Exception classNot) {
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

    public void init(List<String> allSongsFromGenre, int songId) {

        // vres ola ta kleidia gia ta opoia eisai upeuthinos
        findValueFromDataset2(allSongsFromGenre, songId);

        System.out.println(publisherValues.toString());

        System.out.print("Publisher with songId " + songId + " is responsible for the following artistNames: ");
        for (Value value : publisherValues) {
            artistNames.add(new ArtistName(value.getMusicFile().getArtistName()));
            System.out.print(value.getMusicFile().getArtistName() + " ");
        }
        System.out.println();
    }

    /***** PUBLISHER METHODS *****/

    public void getBrokerList() {

        String brokersFile = "./Dataset/DS_project_dataset/BrokersList.txt";
//        String brokersFile = "C:\\Users\\nikos\\workspace\\aueb\\distributed systems\\ds-project-2019\\Dataset\\DS_project_dataset\\BrokersList.txt";

        // read file into stream, try-with-resources
        try (Stream<String> stream = Files.lines(Paths.get(brokersFile))) {

            stream.map(line -> {
                String[] fields = line.split(",");
                Broker broker = new Broker(fields[0], Integer.parseInt(fields[1]));
                return broker;
            })
                    .forEach(line -> brokersCluster.add(line));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Broker hashTopic(ArtistName topic) {

        String artistName = topic.getArtistName();
        String sha1Hash = null;// hash the name of file with sha1
        List<Integer> brokerHashesList = new ArrayList<>();

        try {
            sha1Hash = HashGenerator.generateSHA1(artistName);
        } catch (HashGenerationException e) {
            e.printStackTrace();
        }
        int publisherKey = new BigInteger(sha1Hash, 16).intValue(); //convert the hex to big int
        int publisherModKey = Math.abs(publisherKey % 64);
        int brokerKey = 0;
        int brokerModKey = 0;

        for (Broker broker : brokersCluster) {
            String brokerHash = null;// hash the name of file with sha1
            try {
                Broker mybroker = broker;
                brokerHash = HashGenerator.generateSHA1(mybroker.getIpAddress() + mybroker.getPort());
            } catch (HashGenerationException e) {
                e.printStackTrace();
            }
            brokerKey = new BigInteger(brokerHash, 16).intValue(); //convert the hex to big int
            brokerModKey = Math.abs(brokerKey % 64);

            //System.out.println(brokerKey + " " + brokerModKey + " " + publisherKey + " " + publisherModKey);
            brokerHashesList.add(brokerModKey);
        }

        int minDistance = 9999;
        int nodeId = 0;

        for (int i = 0; i < brokerHashesList.size(); i++) { //send the file in the correct(by id) node

            // System.out.println(brokerHashesList.get(i));
            if ((Math.abs(publisherModKey - brokerHashesList.get(i))) < minDistance) {
                minDistance = Math.abs(publisherModKey - brokerHashesList.get((i)));
                nodeId = i;
            }
        }

        Broker broker = brokersCluster.get(nodeId);

//        System.out.println("MinDistance is " + minDistance + " and broker node that should be chosen is " + broker + " with id " + nodeId);
//        System.out.println();

        return broker;
    }

    public void notifyFailure(Broker broker) {

    }

    private void findValueFromDataset2(List<String> allSongsFromGenre, int songId) {

        String songFile = allSongsFromGenre.get(songId);
//        String busLinesFile = "C:\\Users\\nikos\\workspace\\aueb\\distributed systems\\ds-project-2019\\Dataset\\DS_project_dataset\\busLinesNew.txt";

        InputStream input = null;
        try {
            input = new FileInputStream(new File(songFile));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        byte[] musicFileExtract = null;
        ContentHandler handler = new DefaultHandler();
        Metadata metadata = new Metadata();
        Parser parser = new Mp3Parser();
        ParseContext parseCtx = new ParseContext();
        try {
            parser.parse(input, handler, metadata, parseCtx);
            musicFileExtract = readAudioFileData(songFile);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (TikaException e) {
            e.printStackTrace();
        }
        try {
            input.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // List all metadata
        String[] metadataNames = metadata.names();

        for (String name : metadataNames) {
            System.out.println(name + ": " + metadata.get(name));
        }

        // Retrieve the necessary info from metadata
        // Names - title, xmpDM:artist etc. - mentioned below may differ based
        System.out.println("----------------------------------------------");
        String trackName = metadata.get("title");
        String artistName = metadata.get("xmpDM:artist");
        String albumInfo = metadata.get("xmpDM:album");
        String genre = metadata.get("xmpDM:genre");

        // add value to list

        MusicFile musicFile = new MusicFile(trackName, artistName, albumInfo, genre, musicFileExtract);
        ArtistName artistName1 = new ArtistName(artistName);
        Value value = new Value(musicFile);

        System.out.println(musicFile);
        publisherValues.add(value);
    }

    public byte[] readAudioFileData(String filePath) throws IOException {

        byte[] data = null;
        try {
            ByteArrayOutputStream baout = new ByteArrayOutputStream();
            File file = new File(filePath);
            AudioInputStream in = AudioSystem.getAudioInputStream(file);
            AudioInputStream din = null;
            AudioFormat baseFormat = in.getFormat();
            AudioFormat decodedFormat = new AudioFormat(AudioFormat.Encoding.PCM_SIGNED,
                    baseFormat.getSampleRate(),
                    16,
                    baseFormat.getChannels(),
                    baseFormat.getChannels() * 2,
                    baseFormat.getSampleRate(),
                    false);
            din = AudioSystem.getAudioInputStream(decodedFormat, in);

            byte[] buffer = new byte[4096];
            int c;
            while ((c = din.read(buffer, 0, buffer.length)) != -1) {
                baout.write(buffer, 0, c);
            }
            din.close();
            baout.close();
            data = baout.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return data;
    }

    private List<String> getAllSongsFromGenre(String directory) {

        try (Stream<Path> walk = Files.walk(Paths.get(directory))) {

            List<String> result = walk.filter(Files::isRegularFile)
                    .map(x -> x.toString()).collect(Collectors.toList());

            result.sort(Comparator.comparing(String::toString));
            result.forEach(System.out::println);

            return result;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new ArrayList<>();
    }

    @Override
    public String toString() {

        return "Publisher{" +
                "addr='" + addr + '\'' +
                ", port=" + port +
                '}';
    }
}