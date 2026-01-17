package com.example.taxi.job;

import com.example.taxi.model.TaxiRide;
import com.example.taxi.source.TaxiRideSource;
import com.example.taxi.sink.ElasticsearchHttpSink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.util.HashMap;
import java.util.Map;

public class PopularDropoffJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        var rides = TaxiRideSource.fromFile(env, "data/nycTaxiData.gz");

        var counts = rides
                .filter(ride -> !ride.isStart)
                .map(ride -> new Tuple2<>(ride.dropoffLocation, 1))
                .returns(TypeInformation.of(Tuple2.class))
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .sum(1);
        counts.addSink(new ElasticsearchHttpSink("http://localhost:9200/taxi-popular-dropoffs"));

        env.execute("Popular Taxi Dropoff Locations");
    }
}
