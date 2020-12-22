package com.wnswdwy.app;


import com.wnswdwy.bean.ChannelBehaviorCount;
import com.wnswdwy.bean.MarketUserBehavior;
import com.wnswdwy.source.MarketBehaviorSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author yycstart
 * @create 2020-12-21 15:36
 */
public class channelApp {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2. 从自定义数据获取数据，并过滤
        SingleOutputStreamOperator<MarketUserBehavior>  filterDS = env.addSource(new MarketBehaviorSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MarketUserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(MarketUserBehavior element) {
                        return element.getTimestamp();
                    }
                }).filter(data -> "UNINSTALL".equals(data.getBehavior()));

        //3. 分组，开窗，
        WindowedStream<MarketUserBehavior, Tuple, TimeWindow> windowedStream = filterDS.keyBy("channel", "behavior")
                .timeWindow(Time.hours(1), Time.seconds(5));
        //4. 聚合
        SingleOutputStreamOperator<ChannelBehaviorCount> result = windowedStream.aggregate(new MarketCountAgg(), new MarketCountResult());
        //5.打印
        result.print();
        //6. 执行
        env.execute();
    }

    public static class MarketCountAgg implements AggregateFunction<MarketUserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(MarketUserBehavior marketUserBehavior, Long aLong) {
            return aLong;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }


    public static class MarketCountResult implements WindowFunction<Long, ChannelBehaviorCount, Tuple, TimeWindow>{

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ChannelBehaviorCount> out) throws Exception {
            String channel = tuple.getField(0);
            String behavior = tuple.getField(1);
            String windowEnd = new Timestamp(window.getEnd()).toString();
            Long count = input.iterator().next();


            out.collect(new ChannelBehaviorCount(channel,behavior,windowEnd,count));
        }
    }
}
