package org.apache.flink.hourlytips;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.common.datatypes.TaxiFare;
import org.apache.flink.common.sources.TaxiFareGenerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author Elias (siran0611@gmail.com)
 */
public class HourlyTips {

    private final SourceFunction<TaxiFare> source;
    private final SinkFunction<Tuple3<Long,Long,Float>> sink;

    public HourlyTips(
            SourceFunction<TaxiFare> source,
            SinkFunction<Tuple3<Long, Long, Float>> sink) {
        this.source = source;
        this.sink = sink;
    }

    public static void main(String[] args) throws Exception{
        HourlyTips job = new HourlyTips(new TaxiFareGenerator(), new PrintSinkFunction<>());
        job.execute();
    }

    public JobExecutionResult execute() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TaxiFare> fares =
                env.addSource(source)
                        .assignTimestampsAndWatermarks(
                                // taxi fares are in order
                                WatermarkStrategy.<TaxiFare>forMonotonousTimestamps()
                                        .withTimestampAssigner(
                                                (fare, t) -> fare.getEventTimeMillis()));

        DataStream<Tuple3<Long,Long,Float>> hourlyTips = fares.keyBy((TaxiFare fare) -> fare.driverId)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new AddTips());

        SingleOutputStreamOperator<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .maxBy(2);

        hourlyMax.addSink(sink);

        return env.execute("Hourly Tips");
    }

    public static class AddTips extends ProcessWindowFunction<TaxiFare, Tuple3<Long,Long,Float>,Long, TimeWindow> {

        @Override
        public void process(
                Long key,
                ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow>.Context context,
                Iterable<TaxiFare> fares,
                Collector<Tuple3<Long, Long, Float>> out) throws Exception {
            float sumOfTips = 0F;
            for (TaxiFare f: fares) {
                sumOfTips += f.tip;
            }
            out.collect(Tuple3.of(context.window().getEnd(), key,sumOfTips));
        }
    }
}
