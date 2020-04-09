package com.itshareplus.googlemapdemo;

import java.io.Serializable;

public class BusPosition implements Serializable {
    private String lineCode;
    private String routeCode;
    private String vehicleId;
    private double latitude;
    private double longitude;
    private String timestampOfBusPosition;

    public BusPosition(String lineCode, String routeCode, String vehicleId, double latitude, double longitude, String timestampOfBusPosition) {
        this.lineCode = lineCode;
        this.routeCode = routeCode;
        this.vehicleId = vehicleId;
        this.latitude = latitude;
        this.longitude = longitude;
        this.timestampOfBusPosition = timestampOfBusPosition;
    }

    public String getLineCode() {
        return lineCode;
    }

    public void setLineCode(String lineCode) {
        this.lineCode = lineCode;
    }

    public String getRouteCode() {
        return routeCode;
    }

    public void setRouteCode(String routeCode) {
        routeCode = routeCode;
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

}