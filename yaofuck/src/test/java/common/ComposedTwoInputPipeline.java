package common;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.common.utils.MissingSolutionException;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author Elias (siran0611@gmail.com)
 */
public class ComposedTwoInputPipeline<IN1, IN2, OUT>
        implements ExecutableTwoInputPipeline<IN1, IN2, OUT> {

    private final ExecutableTwoInputPipeline<IN1, IN2, OUT> exercise;
    private final ExecutableTwoInputPipeline<IN1, IN2, OUT> solution;

    public ComposedTwoInputPipeline(
            ExecutableTwoInputPipeline<IN1, IN2, OUT> exercise,
            ExecutableTwoInputPipeline<IN1, IN2, OUT> solution) {

        this.exercise = exercise;
        this.solution = solution;
    }

    @Override
    public JobExecutionResult execute(
            SourceFunction<IN1> source1, SourceFunction<IN2> source2, TestSink<OUT> sink)
            throws Exception {

        JobExecutionResult result;

        try {
            result = exercise.execute(source1, source2, sink);
        } catch (Exception e) {
            if (MissingSolutionException.ultimateCauseIsMissingSolution(e)) {
                result = solution.execute(source1, source2, sink);
            } else {
                throw e;
            }
        }

        return result;
    }
}
