package com.wnswdwy.app;

import com.wnswdwy.bean.OrderEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author yycstart
 * @create 2020-12-22 18:43
 */
public class OrderTimeOutStateApp {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //2. 从文本数据中读取，转换成JavaBean，并指定时间为时间戳
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

        //3.按照userId分组
        KeyedStream<OrderEvent, Long> keyedStream = orderEventDS.keyBy(OrderEvent::getOrderId);

        //4.使用状态编程实现订单超时功能
        SingleOutputStreamOperator<String> result = keyedStream.process(new MyOrderTimeOutFunc(15));

        //5. 打印数据
        result.print("Result");
        result.getSideOutput(new OutputTag<String>("Payed TimeOut"){}).print("TimeOut");
        result.getSideOutput(new OutputTag<String>("No Pay"){}).print("No Pay");

        //6.执行
        env.execute();
    }

    public static class MyOrderTimeOutFunc extends KeyedProcessFunction<Long,OrderEvent,String>{

        private int diffTime;

        public MyOrderTimeOutFunc(int diffTime) {
            this.diffTime = diffTime;
        }

        //定义状态用于保存创建数据
        ValueState<OrderEvent> createState;
        ValueState<Long> tsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            createState  = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("order-state", OrderEvent.class));
            tsState  = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state", Long.class));
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<String> out) throws Exception {


            if("create".equals(value.getEventType())){
                //获取当前订单时间，并补充设定时间
                Long ts = (value.getEventTime() + diffTime * 60) * 1000L ;
                createState.update(value);
                ctx.timerService().registerEventTimeTimer(ts);
                tsState.update(ts);
            }else if("pay".equals(value.getEventType())){
                //获取当前状态
                OrderEvent orderEvent = createState.value();
                if(orderEvent == null){
                    //为空，说明订单创建了，但15分钟了没有支付
                    ctx.output(new OutputTag<String>("Payed TimeOut"){},diffTime + "分钟已经过去," +value.getOrderId()+"支付超时！!");
                }else{
                    //15分钟内，完成支付
                    out.collect(orderEvent.getOrderId() + "Create at " + orderEvent.getEventTime() + ",Payed At " + value.getEventTime());
                   //删除定时器
                    ctx.timerService().deleteEventTimeTimer(tsState.value());
                    //清空状态
                    tsState.clear();
                    createState.clear();
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //超时未支付
            OrderEvent orderEvent = createState.value();
            ctx.output(new OutputTag<String>("No Pay"){},orderEvent.getOrderId() + " But No Pay");
            //清空状态
            createState.clear();
            tsState.clear();
        }
    }
}
