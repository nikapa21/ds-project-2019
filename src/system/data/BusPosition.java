package system.data;

public class BusPosition {
    private String lineId;
    private String RouteCode;
    private String vehicleId;
    private double latitude;

    public BusPosition(String lineId, String routeCode, String vehicleId, double latitude, double longitude, String timestampOfBusPosition) {
        this.lineId = lineId;
        RouteCode = routeCode;
        this.vehicleId = vehicleId;
        this.latitude = latitude;
        this.longitude = longitude;
        this.timestampOfBusPosition = timestampOfBusPosition;
    }

    public String getLineId() {
        return lineId;
    }

    public void setLineId(String lineId) {
        this.lineId = lineId;
    }

    public String getRouteCode() {
        return RouteCode;
    }

    public void setRouteCode(String routeCode) {
        RouteCode = routeCode;
    }

    public String getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(String vehicleId) {
        this.vehicleId = vehicleId;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public String getTimestampOfBusPosition() {
        return timestampOfBusPosition;
    }

    public void setTimestampOfBusPosition(String timestampOfBusPosition) {
        this.timestampOfBusPosition = timestampOfBusPosition;
    }

    private double longitude;
    private String timestampOfBusPosition;
}