package com.wnswdwy.app;

import com.wnswdwy.bean.OrderEvent;
import com.wnswdwy.bean.ReceiptEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author yycstart
 * @create 2020-12-22 21:17
 */
public class PayReceiptWithJoinApp {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2. 从文本数据读取，并转换成JavaBean,并指定时间为时间戳
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.readTextFile("input/OrderLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(Long.parseLong(fields[0]),
                            fields[1], fields[2],
                            Long.parseLong(fields[3]));
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getEventTime() * 1000L;
                    }
                });
        SingleOutputStreamOperator<ReceiptEvent> receiptEventDS = env.readTextFile("input/ReceiptLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new ReceiptEvent(fields[0], fields[1], Long.parseLong(fields[2]));
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {
                    @Override
                    public long extractAscendingTimestamp(ReceiptEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });
        //3. 将两个DS 按照id分组
        KeyedStream<OrderEvent, String> orderEventKeyedStream = orderEventDS.keyBy(OrderEvent::getTxId);
        KeyedStream<ReceiptEvent, String> receiptEventKeyedStream = receiptEventDS.keyBy(ReceiptEvent::getTxId);

        //4. 使用Join
        KeyedStream.IntervalJoin<OrderEvent, ReceiptEvent, String> joinResult = orderEventKeyedStream.intervalJoin(receiptEventKeyedStream);
        //5. procproc
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> result = joinResult.between(Time.seconds(-3), Time.seconds(5))
                .process(new PayReceiptProcessFunc());
        //6. 打印
        result.print();
        //7.执行
        env.execute();
    }

    public static class PayReceiptProcessFunc extends ProcessJoinFunction<OrderEvent,ReceiptEvent, Tuple2<OrderEvent,ReceiptEvent>>{

        @Override
        public void processElement(OrderEvent left, ReceiptEvent right, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            out.collect(new Tuple2<>(left,right));
        }
    }
}
