package common;

/**
 * @author Elias (siran0611@gmail.com)
 */

import org.apache.flink.common.utils.MissingSolutionException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
public class ComposedKeyedProcessFunction<K, IN, OUT> extends KeyedProcessFunction<K, IN, OUT> {
    private final KeyedProcessFunction<K, IN, OUT> exercise;
    private final KeyedProcessFunction<K, IN, OUT> solution;
    private KeyedProcessFunction<K, IN, OUT> implementationToTest;

    public ComposedKeyedProcessFunction(
            KeyedProcessFunction<K, IN, OUT> exercise, KeyedProcessFunction<K, IN, OUT> solution) {

        this.exercise = exercise;
        this.solution = solution;
        this.implementationToTest = exercise;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        try {
            exercise.setRuntimeContext(this.getRuntimeContext());
            exercise.open(parameters);
        } catch (Exception e) {
            if (MissingSolutionException.ultimateCauseIsMissingSolution(e)) {
                this.implementationToTest = solution;
                solution.setRuntimeContext(this.getRuntimeContext());
                solution.open(parameters);
            } else {
                throw e;
            }
        }
    }

    @Override
    public void processElement(
            IN value, KeyedProcessFunction<K, IN, OUT>.Context ctx, Collector<OUT> out)
            throws Exception {

        implementationToTest.processElement(value, ctx, out);
    }

    @Override
    public void onTimer(
            long timestamp, KeyedProcessFunction<K, IN, OUT>.OnTimerContext ctx, Collector<OUT> out)
            throws Exception {

        implementationToTest.onTimer(timestamp, ctx, out);
    }
}
