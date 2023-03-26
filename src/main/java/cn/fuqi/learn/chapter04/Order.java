package cn.fuqi.learn.chapter04;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * @author fanfanfufu
 * @version 1.0
 * @date 2023/3/26 18:09
 * @description
 */
@Data
@NoArgsConstructor
@AllArgsConstructor(staticName = "of")
public class Order {
    private String id;
    private Integer salary;
    private LocalDateTime produceTime;
}
