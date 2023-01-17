package common;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @author Elias (siran0611@gmail.com)
 */
public class ParallelTestSource<T> extends RichParallelSourceFunction<T>
        implements ResultTypeQueryable<T> {
    private final T[] testStream;
    private final TypeInformation<T> typeInfo;

    @SuppressWarnings("unchecked")
    @SafeVarargs
    public ParallelTestSource(T... events) {
        this.typeInfo = (TypeInformation<T>) TypeExtractor.createTypeInfo(events[0].getClass());
        this.testStream = events;
    }

    @Override
    public void run(SourceContext<T> ctx) {
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        int subtask = 0;

        // the elements of the testStream are assigned to the parallel instances in a round-robin
        // fashion
        for (T element : testStream) {
            if (subtask == indexOfThisSubtask) {
                ctx.collect(element);
            }
            subtask = (subtask + 1) % numberOfParallelSubtasks;
        }

        // test sources are finite, so they emit a Long.MAX_VALUE watermark when they finish
    }

    @Override
    public void cancel() {
        // ignore cancel, finite anyway
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return typeInfo;
    }
}
