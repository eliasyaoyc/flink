package org.apache.flink.ridesandfares;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.common.datatypes.RideAndFare;
import org.apache.flink.common.datatypes.TaxiFare;
import org.apache.flink.common.datatypes.TaxiRide;
import org.apache.flink.common.sources.TaxiFareGenerator;
import org.apache.flink.common.sources.TaxiRideGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * @author Elias (siran0611@gmail.com)
 */
public class RidesAndFares {
    private final SourceFunction<TaxiRide> rideSource;
    private final SourceFunction<TaxiFare> fareSource;
    private final SinkFunction<RideAndFare> sink;

    public RidesAndFares(
            SourceFunction<TaxiRide> rideSource,
            SourceFunction<TaxiFare> fareSource,
            SinkFunction<RideAndFare> sink) {
        this.rideSource = rideSource;
        this.fareSource = fareSource;
        this.sink = sink;
    }

    public JobExecutionResult execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TaxiRide> rides = env
                .addSource(rideSource)
                .filter(ride -> ride.isStart)
                .keyBy(ride -> ride.rideId);

        DataStream<TaxiFare> fares = env
                .addSource(fareSource)
                .keyBy(fare -> fare.rideId);

        rides.connect(fares).flatMap(new EnrichmentFunction()).addSink(sink);

        return env.execute("Join Rides with Fares");
    }

    public static void main(String[] args) throws Exception{
        RidesAndFares job = new RidesAndFares(
                new TaxiRideGenerator(),
                new TaxiFareGenerator(),
                new PrintSinkFunction<>());

        job.execute();
    }

    public static class EnrichmentFunction
            extends RichCoFlatMapFunction<TaxiRide, TaxiFare, RideAndFare> {

        private ValueState<TaxiRide> rideState;
        private ValueState<TaxiFare> fareState;

        @Override
        public void open(Configuration config) {
            rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class));
            fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class));
        }

        @Override
        public void flatMap1(TaxiRide ride, Collector<RideAndFare> out) throws Exception {
            TaxiFare fare = fareState.value();
            if (fare != null) {
                fareState.clear();
                out.collect(new RideAndFare(ride,fare));
            }else {
                rideState.update(ride);
            }
        }

        @Override
        public void flatMap2(TaxiFare fare, Collector<RideAndFare> out) throws Exception {
            TaxiRide ride = rideState.value();
            if (ride != null) {
                rideState.clear();
                out.collect(new RideAndFare(ride,fare));
            }else {
                fareState.update(fare);
            }
        }
    }
}
