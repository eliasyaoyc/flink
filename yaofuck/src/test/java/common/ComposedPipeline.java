package common;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.common.utils.MissingSolutionException;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author Elias (siran0611@gmail.com)
 */
public class ComposedPipeline<IN, OUT> implements ExecutablePipeline<IN, OUT> {

    private final ExecutablePipeline<IN, OUT> exercise;
    private final ExecutablePipeline<IN, OUT> solution;

    public ComposedPipeline(
            ExecutablePipeline<IN, OUT> exercise, ExecutablePipeline<IN, OUT> solution) {
        this.exercise = exercise;
        this.solution = solution;
    }

    @Override
    public JobExecutionResult execute(SourceFunction<IN> source, TestSink<OUT> sink)
            throws Exception {

        JobExecutionResult result;

        try {
            result = exercise.execute(source, sink);
        } catch (Exception e) {
            if (MissingSolutionException.ultimateCauseIsMissingSolution(e)) {
                result = solution.execute(source, sink);
            } else {
                throw e;
            }
        }

        return result;
    }
}
