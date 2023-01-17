package cn.fuqi.learn.chapter02;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author FuQi
 * @date 2023/1/17 21:18
 * @description 基于集合的source使用学习
 */
public class CollectionSourceLearn {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // source
        DataStreamSource<String> elements = env.fromElements("flink", "source", "element", "learn");
        DataStreamSource<String> collection = env.fromCollection(Stream.of("hello", "world", "happy", "leetcode", "codeTop").collect(Collectors.toList()));
        DataStreamSource<Long> sequence = env.fromSequence(1, 15);

        // transformation
        // sink
        elements.name("element source").print();
        collection.name("collection source").print();
        sequence.name("sequence source").print();

        // execute0
        env.execute("collection source learn");
    }
}
