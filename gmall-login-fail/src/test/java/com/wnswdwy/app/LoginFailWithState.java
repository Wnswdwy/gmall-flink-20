package com.wnswdwy.app;

import com.wnswdwy.bean.LoginEvent;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

public class LoginFailWithState {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文本数据转换为JavaBean并提取时间戳生成Watermark
        SingleOutputStreamOperator<LoginEvent> loginEventDS = env.readTextFile("input/LoginLog.csv")
                .map(new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new LoginEvent(Long.parseLong(fields[0]),
                                fields[1],
                                fields[2],
                                Long.parseLong(fields[3]));
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        //3.按照用户id分组
        KeyedStream<LoginEvent, Long> keyedStream = loginEventDS.keyBy(LoginEvent::getUserId);

        //4.使用ProcessFunction处理数据
        SingleOutputStreamOperator<String> result = keyedStream.process(new LoginFailProcessFunc(2));

        //5.打印数据
        result.print();

        //6.执行
        env.execute();

    }

    public static class LoginFailProcessFunc extends KeyedProcessFunction<Long, LoginEvent, String> {

        //定义属性
        private int interval;

        public LoginFailProcessFunc(int interval) {
            this.interval = interval;
        }

        //定义状态
        private ListState<LoginEvent> failListState;
        private ValueState<Long> tsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            failListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("list-state", LoginEvent.class));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state", Long.class));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {

            //获取状态数据
            Iterator<LoginEvent> iterator = failListState.get().iterator();
            Long ts = tsState.value();

            //如果当前数据为失败数据
            if ("fail".equals(value.getEventType())) {

                //将当前数据加入状态
                failListState.add(value);

                //判断当前是否为第一条失败数据
                if (!iterator.hasNext()) {
                    //注册 interval 后的定时器
                    long curTs = (value.getTimestamp() + interval) * 1000L;
                    ctx.timerService().registerEventTimeTimer(curTs);
                    //更新时间状态
                    tsState.update(curTs);
                }
            } else {

                //成功数据,判断定时器是否已经注册过
                if (ts != null) {
                    //删除定时器
                    ctx.timerService().deleteEventTimeTimer(ts);
                }

                //清空状态
                failListState.clear();
                tsState.clear();
            }

        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            //取出状态数据
            ArrayList<LoginEvent> loginEvents = Lists.newArrayList(failListState.get().iterator());

            //判断当前集合中数据的条数
            if (loginEvents.size() >= 2) {
                LoginEvent firstFail = loginEvents.get(0);
                LoginEvent lastFail = loginEvents.get(loginEvents.size() - 1);
                out.collect(ctx.getCurrentKey() +
                        "在" +
                        firstFail.getTimestamp() +
                        "到" +
                        lastFail.getTimestamp() +
                        "之间连续登录失败" +
                        loginEvents.size() +
                        "次");
            }

            //清空状态
            failListState.clear();
            tsState.clear();

        }
    }

}
