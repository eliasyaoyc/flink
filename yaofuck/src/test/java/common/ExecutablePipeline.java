package common;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author Elias (siran0611@gmail.com)
 */
public interface ExecutablePipeline<IN, OUT> {
    JobExecutionResult execute(SourceFunction<IN> source, TestSink<OUT> sink) throws Exception;
}
