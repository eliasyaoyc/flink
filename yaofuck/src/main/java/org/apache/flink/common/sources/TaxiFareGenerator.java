package org.apache.flink.common.sources;

import org.apache.flink.common.datatypes.TaxiFare;
import org.apache.flink.common.utils.DataGenerator;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Duration;
import java.time.Instant;

/**
 * @author Elias (siran0611@gmail.com)
 */
public class TaxiFareGenerator implements SourceFunction<TaxiFare> {

    private volatile boolean running = true;
    private Instant limitingTimestamp = Instant.MAX;

    /** Create a bounded TaxiFareGenerator that runs only for the specified duration. */
    public static TaxiFareGenerator runFor(Duration duration) {
        TaxiFareGenerator generator = new TaxiFareGenerator();
        generator.limitingTimestamp = DataGenerator.BEGINNING.plus(duration);
        return generator;
    }

    @Override
    public void run(SourceContext<TaxiFare> ctx) throws Exception {

        long id = 1;

        while (running) {
            TaxiFare fare = new TaxiFare(id);

            // don't emit events that exceed the specified limit
            if (fare.startTime.compareTo(limitingTimestamp) >= 0) {
                break;
            }

            ++id;
            ctx.collect(fare);

            // match our event production rate to that of the TaxiRideGenerator
            Thread.sleep(TaxiRideGenerator.SLEEP_MILLIS_PER_EVENT);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
