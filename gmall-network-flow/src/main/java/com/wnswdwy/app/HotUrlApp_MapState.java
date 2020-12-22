package com.wnswdwy.app;

import com.wnswdwy.bean.ApacheLog;
import com.wnswdwy.bean.UrlCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

/*
 * @author yycstart
 * @create 2020-12-19 14:17
*/

public class HotUrlApp_MapState {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.提取文本数据
        SingleOutputStreamOperator<ApacheLog> streamOperator = env.readTextFile("input/apache.log")
                .map(new MapFunction<String, ApacheLog>() {
                    @Override
                    public ApacheLog map(String s) throws Exception {
                        SimpleDateFormat ds = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                        String[] fields = s.split(" ");
                        return new ApacheLog(fields[0],
                                fields[1],
                                ds.parse(fields[3]).getTime(),
                                fields[5],
                                fields[6]);
                    }
                }).filter(data -> "GET".equals(data.getMethod()))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLog>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(ApacheLog apacheLog) {
                        return apacheLog.getEventTime();
                    }
                });

        //3.按照UserID分组
        KeyedStream<ApacheLog, String> keyedStream = streamOperator.keyBy(ApacheLog::getUrl);

        //4. 开窗,窗口大小10min，步长5s，允许迟到1s
        WindowedStream<ApacheLog, String, TimeWindow> windowWindowedStream = keyedStream.timeWindow(Time.minutes(10), Time.seconds(5)).allowedLateness(Time.seconds(1));

        //计算每个窗口内部每个URL被访问的次数，滚动集合，WindowFunction提取窗口信息
        SingleOutputStreamOperator<UrlCount> apacheLogDS = windowWindowedStream.aggregate(new UrlCountAggFunc(), new UrlCountWindowFunc());
        //6.按照窗口信息重新分组
        KeyedStream<UrlCount, Long> urlCountDS = apacheLogDS.keyBy(UrlCount::getWindowEnd);
        //7.使用procefunc处理排序，状态编程，定时器
        SingleOutputStreamOperator<String> result = urlCountDS.process(new UrlCountProcessFunc());
        //8.打印
        apacheLogDS.print("apacheLog");
        urlCountDS.print("agg");
        result.print("result");
        //9.执行
        env.execute();
    }
    public static class UrlCountAggFunc implements AggregateFunction<ApacheLog, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLog apacheLog, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    public static class UrlCountWindowFunc implements WindowFunction<Long, UrlCount, String, TimeWindow> {

        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<Long> iterable, Collector<UrlCount> collector) throws Exception {
            collector.collect(new UrlCount(s,timeWindow.getEnd(),iterable.iterator().next()));
        }
    }

    //使用procefunc处理排序，状态编程，定时器
    public static class UrlCountProcessFunc extends KeyedProcessFunction<Long, UrlCount, String> {
        private int topN;

        public UrlCountProcessFunc() {
        }

        public UrlCountProcessFunc(int topN) {
            this.topN = topN;
        }

        MapState<String,UrlCount> mapState;
        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, UrlCount>("map-state",String.class,UrlCount.class));
        }


        @Override
        public void processElement(UrlCount urlCount, Context context, Collector<String> collector) throws Exception {
            //进来数据加入
            mapState.put(urlCount.getUrl(),urlCount);

            Long windowEnd = urlCount.getWindowEnd();

            //注册 1 毫秒的定时器,用于关闭窗口
            context.timerService().registerEventTimeTimer(windowEnd + 1);
            //注册 10 分钟的定时器
            context.timerService().registerEventTimeTimer(windowEnd + 60000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            if(timestamp == ctx.getCurrentKey() - 1){
                mapState.clear();
                return;
            }
            //取出处状态中的数据
            Iterator<Map.Entry<String, UrlCount>> iterator = mapState.entries().iterator();
            ArrayList<Map.Entry<String, UrlCount>> entries = Lists.newArrayList(iterator);
            entries.sort(new Comparator<Map.Entry<String, UrlCount>>() {
                @Override
                public int compare(Map.Entry<String, UrlCount> o1, Map.Entry<String, UrlCount> o2) {
                    if(o1.getValue().getCount() > o2.getValue().getCount()){
                        return -1;
                    }else if(o1.getValue().getCount() < o2.getValue().getCount()){
                        return 1;
                    }else{
                        return 0;
                    }
                }
            });


            //准备输出数据
            StringBuilder sb = new StringBuilder();
            sb.append("=============").append(new Timestamp(timestamp -1)).append("=============").append("\n");
            //遍历数据取出TopN
            for (int i = 0; i < Math.min(topN,entries.size()); i++) {
                Map.Entry<String, UrlCount> stringUrlCountEntry = entries.get(i);
                sb.append("Top ").append(i+1).append(" URL: ").append(stringUrlCountEntry.getValue().getCount())
                        .append(" Count:").append(stringUrlCountEntry.getValue().getCount()).append("\n");
            }

            //休息一会
            Thread.sleep(100);

            //输出数据
            out.collect(sb.toString());
        }
    }
}
