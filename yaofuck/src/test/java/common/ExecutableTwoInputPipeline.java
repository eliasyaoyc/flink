package common;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author Elias (siran0611@gmail.com)
 */
public interface ExecutableTwoInputPipeline<IN1, IN2, OUT> {
    JobExecutionResult execute(
            SourceFunction<IN1> source1, SourceFunction<IN2> source2, TestSink<OUT> sink)
            throws Exception;
}
