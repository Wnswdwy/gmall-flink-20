package com.wnswdwy.app;


import com.wnswdwy.bean.ItemCount;
import com.wnswdwy.bean.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

/**
 * @author yycstart
 * @create 2020-12-19 10:14
 */
public class HotItemApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文本数据，创建流，转换成JavaBean，并提取数据中时间生成的时间戳
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = env.readTextFile("input/UserBehavior.csv").map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] fields = value.split(",");
                return new UserBehavior(Long.parseLong(fields[0]),
                        Long.parseLong(fields[1]),
                        Integer.parseInt(fields[2]),
                        fields[3],
                        Long.parseLong(fields[4]));
            }
        }).filter(data -> "pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior userBehavior) {
                        return userBehavior.getTimestamp() * 1000L;
                    }
                });
        //3.按照商品id分组
        KeyedStream<UserBehavior, Long> keyedStream = userBehaviorDS.keyBy(UserBehavior::getItemId);
        //4.开窗，滑动窗口，窗口大小 1h，滑动步长 5 minutes
        WindowedStream<UserBehavior, Long, TimeWindow> windowedStream = keyedStream.timeWindow(Time.hours(1), Time.minutes(5));
        //5. 聚合计算，计算窗口内部每个商品被点击的次数（聚合可以获取窗口内每个商品，窗口函数可以保证拿到窗口关闭时间）
        SingleOutputStreamOperator<ItemCount> aggregate = windowedStream.aggregate(new ItemCountAggFunc(), new ItemCountWindowFunc());
        //6.按照窗口时间分组
        KeyedStream<ItemCount, Long> itemCountLongKeyedStream = aggregate.keyBy(ItemCount::getWindowEnd);
        //7.使用ProcessFunction实现收集每个窗口中的数据做排序输出(状态编程--ListState  定时器)
        SingleOutputStreamOperator<String> process = itemCountLongKeyedStream.process(new ItemKeyedProcessFunc(5));
        //8.打印输出
        process.print();
        //9.执行
        env.execute();

    }


    //滚动聚合保证来一条计算一条
    public static class ItemCountAggFunc implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1L;
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


    //窗口函数保证可以拿到窗口时间
    public static class ItemCountWindowFunc implements WindowFunction<Long, ItemCount, Long, TimeWindow> {

        @Override
        public void apply(Long itemId, TimeWindow window, Iterable<Long> input, Collector<ItemCount> out) throws Exception {

            //获取key --> itemId,获取窗口的结束时间,获取当前窗口中当前ItemID的点击次数
            out.collect(new ItemCount(itemId, window.getEnd(), input.iterator().next()));

        }
    }




    //使用ProcessFunction实现收集每个窗口中的数据做排序输出(状态编程--ListState  定时器)
    public static class ItemKeyedProcessFunc extends KeyedProcessFunction<Long, ItemCount, String>{
       private int sinkTop;

        public ItemKeyedProcessFunc() {
        }

        public ItemKeyedProcessFunc(int sinkTop) {
            this.sinkTop = sinkTop;
        }

        //声明集合状态
        private ListState<ItemCount> listState;
        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<ItemCount>("list-state", ItemCount.class));
        }

        @Override
        public void processElement(ItemCount itemCount, Context context, Collector<String> collector) throws Exception {
            //来一条数据则加入状态
            listState.add(itemCount);
            //注册定时器
            context.timerService().registerEventTimeTimer(itemCount.getWindowEnd() + 1000L);
        }

        //定时器触发
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //获取状态中的数据
            Iterator<ItemCount> iterator = listState.get().iterator();
            ArrayList<ItemCount> itemCounts = Lists.newArrayList(iterator);

            //排序
            itemCounts.sort(new Comparator<ItemCount>() {
                @Override
                public int compare(ItemCount o1, ItemCount o2) {
                    if(o1.getCount()>o2.getCount()) {
                        return -1;
                    }else if(o1.getCount() < o2.getCount()){
                        return 1;
                    }else {
                        return  0;
                    }
                }
            });
            //输出TopicSize数据
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("=============")
                    .append(new Timestamp(timestamp - 1000L))
                    .append("=============")
                    .append("\n");

            //遍历输出后的结果
            for (int i = 0; i < Math.min(itemCounts.size(),sinkTop); i++) {
                //a.提取数据
                ItemCount itemCount = itemCounts.get(i);

                //b. 封装Top数据
                stringBuilder.append("Top ")
                        .append(i+1)
                        .append(" ItemId: ")
                        .append(itemCount.getItemId())
                        .append(" Count: ")
                        .append(itemCount.getCount())
                        .append("\n");
            }

            //休息
            Thread.sleep(2000);
            //状态清空
            listState.clear();
            //写出数据
            out.collect(stringBuilder.toString());
        }
    }


}
