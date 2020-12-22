package com.wnswdwy.app;

import com.wnswdwy.bean.OrderEvent;
import com.wnswdwy.bean.ReceiptEvent;
import kafka.controller.LogDirEventNotification;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author yycstart
 * @create 2020-12-22 19:45
 */
public class PayReceiptWithConnectApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文本数据创建流,转换为JavaBean并提取时间戳生成Watermark
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.readTextFile("input/OrderLog.csv")
//        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(Long.parseLong(fields[0]),
                            fields[1],
                            fields[2],
                            Long.parseLong(fields[3]));
                })
                .filter(data -> "pay".equals(data.getEventType()))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getEventTime() * 1000L;
                    }
                });

        SingleOutputStreamOperator<ReceiptEvent> receiptEventDS = env.readTextFile("input/ReceiptLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new ReceiptEvent(fields[0], fields[1], Long.parseLong(fields[2]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {
                    @Override
                    public long extractAscendingTimestamp(ReceiptEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });


        //3.按照事务ID分组之后进行连接
        ConnectedStreams<OrderEvent, ReceiptEvent> connectedStreams = orderEventDS.keyBy(OrderEvent::getTxId).connect(receiptEventDS.keyBy(ReceiptEvent::getTxId));

        //4.使用ProcessFunction处理2个流的数据
        SingleOutputStreamOperator<String> result = connectedStreams.process(new PayReceiptKeyedProcessFunc());

        //5.打印数据
        result.print("Payed And Receipt");
        result.getSideOutput(new OutputTag<String>("Payed No Receipt") {
        }).print("Payed No Receipt");
        result.getSideOutput(new OutputTag<String>("No Payed But Receipt") {
        }).print("No Payed But Receipt");

        //6.执行
        env.execute();
    }

    public static class PayReceiptKeyedProcessFunc extends KeyedCoProcessFunction<String, OrderEvent, ReceiptEvent, String> {
        ValueState<OrderEvent> orderEventState;
        ValueState<ReceiptEvent> receiptEventState;
        ValueState<Long> tsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            orderEventState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("orderEvent-state", OrderEvent.class));
            receiptEventState = getRuntimeContext().getState(new ValueStateDescriptor<ReceiptEvent>("orderEvent-state", ReceiptEvent.class));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-State", Long.class));
        }

        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
            //已支付，判断到账情况
            ReceiptEvent receiptEvent = receiptEventState.value();
            if (receiptEvent != null) {
                //支付比到账先，正常输出

                out.collect(value.getTxId() + "Payed And Receipt");
                //删除定时器
                ctx.timerService().deleteEventTimeTimer(tsState.value());
                //清空状态
                tsState.clear();
                receiptEventState.clear();
            } else {
                //已支付但没有到账
                //保存支付状态
                orderEventState.update(value);
                //设定时器
                Long ts = (value.getEventTime() + 5) * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                tsState.update(ts);
            }
        }

        @Override
        public void processElement2(ReceiptEvent value, Context ctx, Collector<String> out) throws Exception {
            //已到账，判断支付情况
            OrderEvent orderEvent = orderEventState.value();
            if (orderEvent != null) {
                //指定时间内，支付和账单都到账了
                out.collect(value.getTxId() + "Payed And Receipt");
                //删除定时器
                ctx.timerService().deleteEventTimeTimer(tsState.value());
                //清空状态
                tsState.clear();
                orderEventState.clear();
            } else {
                //账单已到，但还未支付
                //保存账单状态
                receiptEventState.update(value);
                //设定时器
                Long ts = (value.getTimestamp() + 3) * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                tsState.update(ts);
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //订单已到达，判断支付是否为空
            OrderEvent orderEvent = orderEventState.value();
            ReceiptEvent receiptEvent = receiptEventState.value();
            if(orderEvent != null){
                //支付了，但还没到账
                ctx.output(new OutputTag<String>("Payed No Receipt"){},orderEvent.getTxId() + "Payed But No Receipt");
            }
            if(receiptEvent == null){

                //账单到了，还没有支付
                ctx.output(new OutputTag<String>("No Payed But Receipt"){},receiptEvent.getTxId() + "No Pay But Receipt");
            }

            //情空状态
            orderEventState.clear();
            receiptEventState.clear();
        }
    }
}
