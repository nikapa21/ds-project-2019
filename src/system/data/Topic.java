package system.data;

public class Topic {
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
}
