package com.example.taxi.model;

public class TaxiRide {
    public long rideId;
    public boolean isStart;
    public int dropoffLocation;

    public TaxiRide() {}

    public TaxiRide(long rideId, boolean isStart, int dropoffLocation) {
        this.rideId = rideId;
        this.isStart = isStart;
        this.dropoffLocation = dropoffLocation;
    }
}