package org.apache.flink.ridecleansing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.common.datatypes.TaxiRide;
import org.apache.flink.common.sources.TaxiRideGenerator;
import org.apache.flink.common.utils.GeoUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author Elias (siran0611@gmail.com)
 */
public class RideCleansingSolution {

    private final SourceFunction<TaxiRide> source;
    private final SinkFunction<TaxiRide> sink;

    public RideCleansingSolution(SourceFunction<TaxiRide> source, SinkFunction<TaxiRide> sink) {
        this.source = source;
        this.sink = sink;
    }

    public static void main(String[] args) throws Exception {
        RideCleansingSolution job =
                new RideCleansingSolution(new TaxiRideGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    /**
     * Creates and executes the long rides pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the pipeline
        env.addSource(source).filter(new NYCFilter()).addSink(sink);

        // run the pipeline and return the result
        return env.execute("Taxi Ride Cleansing");
    }

    public static class NYCFilter implements FilterFunction<TaxiRide> {

        @Override
        public boolean filter(TaxiRide taxiRide) throws Exception {
            return GeoUtils.isInNYC(taxiRide.startLon,taxiRide.startLat)
                    && GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
        }
    }
}
