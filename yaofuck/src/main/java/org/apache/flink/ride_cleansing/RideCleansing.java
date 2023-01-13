package org.apache.flink.ride_cleansing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.common.datatypes.TaxiRide;
import org.apache.flink.common.sources.TaxiRideGenerator;
import org.apache.flink.common.utils.MissingSolutionException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author Elias (siran0611@gmail.com)
 */
public class RideCleansing {
    private final SourceFunction<TaxiRide> source;
    private final SinkFunction<TaxiRide> sink;

    public RideCleansing(SourceFunction<TaxiRide> source, SinkFunction<TaxiRide> sink) {
        this.source = source;
        this.sink = sink;
    }

    public static void main(String[] args) throws Exception{
        RideCleansing job = new RideCleansing(new TaxiRideGenerator(),
                new PrintSinkFunction<>());
        job.execute();
    }

    public JobExecutionResult execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(source).filter(new NYCFilter()).addSink(sink);

        return env.execute("Taxi Ride Cleansing");
    }

    public static class NYCFilter implements FilterFunction<TaxiRide> {

        @Override
        public boolean filter(TaxiRide value) throws Exception {
            throw new MissingSolutionException();
        }
    }
}
