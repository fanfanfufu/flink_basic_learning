package cn.fuqi.learn.chapter04;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author fanfanfufu
 * @version 1.0
 * @date 2023/3/26 18:24
 * @description
 */
public class CusTomSourceLearn {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Order> singleOutputStreamOperator = env.addSource(new MyCustomSource()).name("custom order source").setParallelism(2);
        singleOutputStreamOperator.name("easy consumer").print();

        env.execute("custom source learn");
    }
}
