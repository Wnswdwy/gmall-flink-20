package com.wnswdwy.app;

import com.wnswdwy.bean.LoginEvent;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author yycstart
 * @create 2020-12-21 16:18
 */
public class LoginFailWithState {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2. 从文件获取数据并转成JavaBean,并提取时间生成时间戳
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

        //3. 按照id 分组
        KeyedStream<LoginEvent, Long> keyedStream = loginEventDS.keyBy(LoginEvent::getUserId);
        //4.使用ProcessFunction处理数据
        SingleOutputStreamOperator<String> Result = keyedStream.process(new LoginFailProcFunc(2));
        //5. 打印
        Result.print();
        //6. 执行
        env.execute();
    }

    public static class LoginFailProcFunc extends KeyedProcessFunction<Long,LoginEvent,String>{

        private int interval;

        public LoginFailProcFunc(int interval) {
            this.interval = interval;
        }
        ListState<LoginEvent> failListState;
        ValueState<Long> tsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            failListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("list-state", LoginEvent.class));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state", Long.class));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {
            Long ts = tsState.value();
            Iterator<LoginEvent> iterator = failListState.get().iterator();
            //判断
            if("fail".equals(value.getEventType())){
                //将当前数据加入状态
                failListState.add(value);

                //判断当前是否为第一条失败数据
                if(!iterator.hasNext()){
                    //注册 interval 后的定时器
                    long curTs = (value.getTimestamp() + interval) * 1000L;
                    ctx.timerService().registerEventTimeTimer(curTs);
                    //更新时间状态
                    tsState.update(curTs);
                }else{
                    //成功数据，判断定时器是否注册过
                    if(ts != null){
                        //删除定时器
                        ctx.timerService().deleteEventTimeTimer(ts);
                    }

                    //清空状态
                    failListState.clear();
                    tsState.clear();
                }
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<LoginEvent> loginEvents = Lists.newArrayList(failListState.get().iterator());
            if(loginEvents.size() >= interval){
                LoginEvent firstFail = loginEvents.get(0);
                LoginEvent lastFail = loginEvents.get(interval - 1);

                out.collect(ctx.getCurrentKey() + "在" + firstFail + "~"+ lastFail + "之间连续登录失败" + interval);
            }

            tsState.clear();
            failListState.clear();
        }
    }
}
