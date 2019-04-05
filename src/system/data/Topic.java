package system.data;

import java.io.Serializable;

public class Topic implements Serializable {
    private String busLine;

    public String getBusLine() {
        return busLine;
    }

    public void setBusLine(String busLine) {
        this.busLine = busLine;
    }

    public Topic(String busLine) {
        this.busLine = busLine;
    }

    @Override
    public String toString() {
        return "Topic{" +
                "busLine='" + busLine + '\'' +
                '}';
    }
}
