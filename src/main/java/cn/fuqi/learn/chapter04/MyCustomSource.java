package cn.fuqi.learn.chapter04;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.time.LocalDateTime;
import java.util.Random;
import java.util.UUID;

/**
 * @author fanfanfufu
 * @version 1.0
 * @date 2023/3/26 18:16
 * @description
 */
public class MyCustomSource extends RichParallelSourceFunction<Order> {
    private Boolean flag = true;

    @Override
    public void run(SourceContext<Order> sourceContext) throws Exception {
        Random random = new Random();
        while (flag) {
            Thread.sleep(1000);
            Order order = Order.of(UUID.randomUUID().toString(), random.nextInt(150001), LocalDateTime.now());
            sourceContext.collect(order);
        }
    }

    @Override
    public void cancel() {
        this.flag = false;
    }
}
