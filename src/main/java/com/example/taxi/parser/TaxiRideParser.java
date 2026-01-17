package com.example.taxi.parser;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.flink.api.common.functions.MapFunction;

import com.example.taxi.model.TaxiRide;

public class TaxiRideParser implements MapFunction<String, TaxiRide> {

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public TaxiRide map(String line) {
        String[] tokens = line.split(",");
        long rideId = Long.parseLong(tokens[0]);
        LocalDateTime timestamp = LocalDateTime.parse(tokens[1], FORMATTER);
        boolean isStart = tokens[2].equalsIgnoreCase("START");
        double lon = Double.parseDouble(tokens[3]);
        double lat = Double.parseDouble(tokens[4]);
        int passengerCount = Integer.parseInt(tokens[5]);
        int taxiId = Integer.parseInt(tokens[6]);

        return new TaxiRide(rideId, timestamp, isStart, lon, lat, passengerCount, taxiId);
    }
}
