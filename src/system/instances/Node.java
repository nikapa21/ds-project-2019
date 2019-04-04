package system.instances;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public interface Node {
    final static List<Broker> brokers = new ArrayList<>();

    void init(int number);
    void connect();
    void disconnect();
    void updateNodes();
    List<Broker> getBrokers();

}
