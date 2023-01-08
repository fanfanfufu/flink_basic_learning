package cn.fuqi.learn.chapter01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author FuQi
 * @date 2023/1/8 20:40
 * @description
 */
public class ChineseWordCount {

    public static void main(String[] args) throws Exception {
        // 1. firstly, set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2. create the chinese text dataset
        DataSet<String> chineseText = env.fromElements(
                "风急天高猿啸哀，渚清沙白鸟飞回。" +
                        "无边落木萧萧下，不尽长江滚滚来。" +
                        "万里悲秋常作客，百年多病独登台。" +
                        "艰难苦恨繁霜鬓，潦倒新停浊酒杯。"
        );
        // 3. split the text string, then group by key, then count the key
        AggregateOperator<Tuple2<String, Integer>> wordCount = chineseText.flatMap(new LineSplitter())
                // the value 0 of the param means the index of data in a tuple data
                .groupBy(0)
                // the value 1 of the param means the index of data in a tuple data
                .sum(1);
        // 4. print the word count
        wordCount.print();
    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word :line.split("")) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
