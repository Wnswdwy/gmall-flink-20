
package com.wnswdwy.app;

import com.wnswdwy.bean.OrderEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/*
 * @author yycstart
 * @create 2020-12-22 14:09
 * 电商网站往往会对订单状态进行监控，
 * 设置一个失效时间（比如15分钟），如果下单后一段时间仍未支付，订单就会被取消
 */

public class OrderTimeOutWithCepApp {
    public static void main(String[] args) throws Exception {

        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2. 从文本数据中读取，并转换成JavaBean,指定时间为时间戳
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.readTextFile("input/OrderLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(Long.parseLong(fields[0]), fields[1], fields[2], Long.parseLong(fields[3]));
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getEventTime() * 1000L;
                    }
                });

        //3. 按照orderId分组
        KeyedStream<OrderEvent, Long> keyedStream = orderEventDS.keyBy(OrderEvent::getOrderId);

        //4.定义模式序列
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("start").
                where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return "create".equals(orderEvent.getEventType());
                    }
                }).followedBy("follow").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent) throws Exception {
                return "pay".equals(orderEvent.getEventType());
            }
        }).within(Time.minutes(15));

        //5.将模式序列应用到流上
        PatternStream<OrderEvent> patternStream = CEP.pattern(keyedStream, pattern);

        //6.提取事件,匹配上的和超时的都需要
        SingleOutputStreamOperator<String> result = patternStream.select(new OutputTag<String>("timeOut"){},
                new MyPatternTimeOutSelectFunc(),
                new MyPatternSelectFunc());

        //7.打印数据
        result.print("Result");
        result.getSideOutput(new OutputTag<String>("timeOut") {
        }).print("TimeOut");

        //8.执行
        env.execute();

    }

    public static class MyPatternTimeOutSelectFunc implements PatternTimeoutFunction<OrderEvent, String> {
        @Override
        public String timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
            OrderEvent start = map.get("start").get(0);
            return start.getOrderId() + "超时！！";
        }
    }

    public static class MyPatternSelectFunc implements PatternSelectFunction<OrderEvent,String>{
        @Override
        public String select(Map<String, List<OrderEvent>> map) throws Exception {
            OrderEvent start = map.get("start").get(0);
            OrderEvent follow = map.get("follow").get(0);
            return start.getOrderId() + " Create at " + start.getEventTime() + ",Payed at " + follow.getEventTime();
        }
    }
}

