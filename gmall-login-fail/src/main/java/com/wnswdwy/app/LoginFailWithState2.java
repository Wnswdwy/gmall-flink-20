package com.wnswdwy.app;


import com.wnswdwy.bean.LoginEvent;
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

/**
 * @author yycstart
 * @create 2020-12-22 11:51
 */
public class LoginFailWithState2 {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2. 从文本获取数据并转换成JavaBean,提取事件作为事件戳
        SingleOutputStreamOperator<LoginEvent> loginEventDS = env.readTextFile("input/LoginLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new LoginEvent(Long.parseLong(fields[0]), fields[1], fields[2], Long.parseLong(fields[3]));
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LoginEvent>() {
                    @Override
                    public long extractAscendingTimestamp(LoginEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        //3. 根据userID分组
        KeyedStream<LoginEvent, Long> keyedStream = loginEventDS.keyBy(LoginEvent::getUserId);

        //4. 通过process方法处理数据
        SingleOutputStreamOperator<String> result = keyedStream.process(new FailProcessFunc(2));

        //5. 打印
        result.print();

        //6.执行
        env.execute();
    }

    public static class FailProcessFunc extends KeyedProcessFunction<Long,LoginEvent,String> {

        private int interval;

        public FailProcessFunc(int interval) {
            this.interval = interval;
        }
        //定义状态，保存失败数据
        ValueState<LoginEvent> failState;
        @Override
        public void open(Configuration parameters) throws Exception {
            failState = getRuntimeContext().getState(new ValueStateDescriptor<>("fail-state", LoginEvent.class));
        }


        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {
            if("fail".equals(value.getEventType())) {
                LoginEvent lastFail = failState.value();
                failState.update(value);
                if (lastFail != null && Math.abs(lastFail.getTimestamp() - value.getTimestamp()) <= 2) {
                    out.collect(lastFail.getUserId() + "在" + lastFail.getTimestamp() +
                            "到" + value.getTimestamp() + "/" + interval + "内连续登录失败2次");
                }
            }else {
                //清空
                failState.clear();
            }
        }
    }
}
