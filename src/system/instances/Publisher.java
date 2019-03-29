package system.instances;

import system.data.*;

import java.util.ArrayList;
import java.util.List;

public class Publisher implements Node{

    List<Topic> topics = new ArrayList<>();

    public Publisher(List<String> list) {
        for (String busLine : list) {
            topics.add(new Topic(busLine));
        }
    }
//    void getBrokerList();
//    Broker hashTopic(Topic topic);
//    void push(Topic topic, Value value);
//    void notifyFailure(Broker broker);



    void init(int port){
        //O
        //publisher node κατά την έναρξη της λειτουργίας του θα πρέπει να γνωρίζει για
        //ποια κλειδιά είναι υπεύθυνος καθώς επίσης και όλη την απαραίτητη
        //πληροφορία για τους brokers. Θα πρέπει να γνωρίζει για κάθε διαθέσιμο
        //broker για ποια κλειδιά είναι υπεύθυνος. Αυτό θα επιτευχθεί κατά τη στιγμή
        //που εκκινείται η λειτουργία του κάθε broker.


        // for example topics responsible for 025, 026, 027

        List<BusLine> publisherBusLines = findFromBusLinesFile(topics);
        List<RouteCode> publisherRouteCodes = findFromRouteCodesFile(publisherBusLines);
        List<BusPosition> publisherBusPositions = findFromBusPositionsFile(publisherRouteCodes);
    }

    private List<BusLine> findFromBusLinesFile(List<Topic> topics) {

    }

    private List<RouteCode> findFromRouteCodesFile(List<BusLine> publisherBusLines) {
    }

    private List<BusPosition> findFromBusPositionsFile(List<RouteCode> publisherRouteCodes) {

    }

//    void connect();
//    void disconnect();
//    void updateNodes();

}
