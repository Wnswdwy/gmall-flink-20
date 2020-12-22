package com.wnswdwy.app;


import com.wnswdwy.bean.PvCount;
import com.wnswdwy.bean.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Random;


/**
 * @author yycstart
 * @create 2020-12-19 19:22
 */
public class PageViewApp_2 {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //        env.setParallelism(8);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文本数据创建流,转换为JavaBean,同时提取数据中的时间戳生成Watermark
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = env.readTextFile("input/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new UserBehavior(Long.parseLong(fields[0]),
                                Long.parseLong(fields[1]),
                                Integer.parseInt(fields[2]),
                                fields[3],
                                Long.parseLong(fields[4]));
                    }
                })
                .filter(data -> "pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        //3.将数据转换为KV结构
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = userBehaviorDS.map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                return new Tuple2<>("pv" + new Random().nextInt(8), 1);
            }
        })
                .keyBy(0);

        //4.开窗处理
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowedStream = keyedStream.timeWindow(Time.hours(1));

        //5.计算WordCount
        SingleOutputStreamOperator<PvCount> pvCountDS = windowedStream.aggregate(new PageViewCountAggFunc(), new PageViewCountWindowFunc());

        //6.按照窗口时间进行分组
        KeyedStream<PvCount, Long> pvCountLongKeyedStream = pvCountDS.keyBy(PvCount::getWindowEnd);

        //7.使用ProcessFunction实现统计一个窗口内部所有数据,状态编程 定时器
        SingleOutputStreamOperator<String> result = pvCountLongKeyedStream.process(new PvCountProcessFunc());

        //8.打印
        result.print();

        //9.执行
        env.execute();


    }


    public static class PageViewCountAggFunc implements AggregateFunction<Tuple2<String, Integer>, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Integer> value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static class PageViewCountWindowFunc implements WindowFunction<Long, PvCount, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<PvCount> out) throws Exception {
            out.collect(new PvCount(window.getEnd(), input.iterator().next()));
        }
    }


    public static class PvCountProcessFunc extends KeyedProcessFunction<Long, PvCount, String> {

        ListState<PvCount> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<PvCount>("list-state",PvCount.class));
        }

        @Override
        public void processElement(PvCount value, Context ctx, Collector<String> out) throws Exception {
            listState.add(value);
            Long windowEnd = value.getWindowEnd();
            ctx.timerService().registerEventTimeTimer(windowEnd + 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Iterator<PvCount> iterator = listState.get().iterator();

            int count = 0;
            while (iterator.hasNext()){
                count += iterator.next().getCount();
            }

            //清空状态
            listState.clear();

            Timestamp windowEnd = new Timestamp(timestamp - 1000L);
            out.collect("窗口结束时间：" + windowEnd + "PvCount:" + count);

        }
    }
}
