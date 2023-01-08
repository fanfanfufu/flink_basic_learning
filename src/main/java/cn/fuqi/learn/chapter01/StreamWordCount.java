package cn.fuqi.learn.chapter01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author FuQi
 * @date 2023/1/8 21:23
 * @description 流式处理的demo，
 * 通过监听socket端口的数据实时对输入数据进行词频统计
 * 提供socket端口需要在Linux系统中使用 nc -lk port 命令，
 * 然后就启动flink任务
 * 然后手动输入任意英文即可实时统计词汇了
 * 最后手动停掉即可停止测试了
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 设置flink当前任务的运行环境
        // 官方文档推荐使用getExecutionEnvironment()方法
        // 该方法可以根据flink任务运行的环境来获取正确的执行环境
        // 当在本地的IDE中像一个常规的Java 程序一样来执行是，就会获取本地环境来在本地机器上执行flink任务
        // 当通过一个jar文件来执行时，就会获取集群环境来执行flink任务
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 建立获取数据的数据源
        DataStreamSource<String> windowDataSource = env.socketTextStream("localhost", 8998, "\n");

        // 3. 数据转换
        // 对实时输入的词汇拆分并统计词频
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCounts = windowDataSource
                .flatMap(new Splitter())
                .keyBy(value -> value.f0)
                .sum(1);

        // 4. 处理转换后的数据：打印到控制台
        wordCounts.name("print the result into the console").print();

        // 5. 执行任务
        env.execute("stream word count");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
