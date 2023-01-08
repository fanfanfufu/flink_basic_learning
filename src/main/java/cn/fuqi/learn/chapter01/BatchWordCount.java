package cn.fuqi.learn.chapter01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author FuQi
 * @date 2023/1/8 21:01
 * @description 批量处理的demo
 * flink 官方文档上的已经没有使用DataSet的相关内容了
 * 不过网上还有大量的demo，可以用来照抄联系一下，看看效果什么的
 */
public class BatchWordCount {
    /**
     * Flink programs look like regular programs that transform DataStreams. Each program consists of the same basic parts:
     * 1. Obtain an execution environment,
     * 2. Load/create the initial data,
     * 3. Specify transformations on this data,
     * 4. Specify where to put the results of your computations,
     * 5. Trigger the program execution
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<String> text = env.fromElements(WordCountData.WORDS).name("in-memory-input");

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCounts = text.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
                    String[] wordArr = line.toLowerCase().split("\\W+");
                    for (String word : wordArr) {
                        out.collect(Tuple2.of(word, 1));
                    }
                }).name("tokenizer").returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1);

        wordCounts.print().name("output word count");

        env.execute();
    }
}
