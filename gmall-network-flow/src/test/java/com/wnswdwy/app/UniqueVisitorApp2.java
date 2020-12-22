package com.wnswdwy.app;

import com.wnswdwy.bean.UserBehavior;
import com.wnswdwy.bean.UvCount;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Timestamp;
import java.util.Iterator;

/**
 * @author yycstart
 * @create 2020-12-21 11:30
 */
public class UniqueVisitorApp2 {
    public static void main(String[] args) throws Exception {
        //1. 获取执行连接
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        //2.从文本中读取数据，并抓换成JavaBean,过滤出 pv 的数据，并时间为时间戳
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = env.readTextFile("input/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String s) throws Exception {
                        String[] fields = s.split(",");
                        return new UserBehavior(Long.parseLong(fields[0]),
                                Long.parseLong(fields[1]),
                                Integer.parseInt(fields[2]),
                                fields[3],
                                Long.parseLong(fields[4]));
                    }
                }).filter(data -> "pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });
        //3. 开窗
        AllWindowedStream<UserBehavior, TimeWindow> windowedStream = userBehaviorDS.timeWindowAll(Time.hours(1));
        //4.自定义触发器，来一处理一
        SingleOutputStreamOperator<UvCount> triggerProcessResult = windowedStream.trigger(new MyTrigger()).process(new MyProcessAllWindowFunc());
        //5.打印
        triggerProcessResult.print();
        //6.执行
        env.execute();
    }

    public static class MyTrigger extends Trigger<UserBehavior,TimeWindow>{
        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }

    public static class MyProcessAllWindowFunc extends ProcessAllWindowFunction<UserBehavior, UvCount,TimeWindow>{

        private Jedis jedis;

        //定义时间Key
        private String hourUvRedisKey;

        private MyBloomFilter myBloomFilter;
        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("hadoop102", 6379);
            hourUvRedisKey = "HourUv";
            myBloomFilter = new MyBloomFilter(1<<30);
        }

        @Override
        public void process(Context context, Iterable<UserBehavior> elements, Collector<UvCount> out) throws Exception {
            //定义窗口关闭时间
            Timestamp windowEnd = new Timestamp(context.window().getEnd());
            //定义位图的Redis Key
            String bitMapRedisKey = "BitMap" + windowEnd;
            //定义每小时的用户访问量 field
            String field = windowEnd.toString();
            //获取当前UserId的对应位置信息
            long offset = myBloomFilter.getOffset(elements.iterator().next().getUserId().toString());
            //查询UserId对应位置信息是否为1
            Boolean getbit = jedis.getbit(bitMapRedisKey, offset);
            //如果为False，则表示给用户第一次访问
            if(!getbit){
                //将对应Bit位 + 1
                jedis.setbit(bitMapRedisKey,offset,true);
                jedis.hincrBy(hourUvRedisKey,field,1L);
            }
            out.collect(new UvCount(field,Long.parseLong(jedis.hget(bitMapRedisKey,field))));

        }

        @Override
        public void close() throws Exception {
            jedis.close();
        }
    }

    //自定义布隆过滤器
    public static class MyBloomFilter{
        //定义传入数据为 2 的整数倍
        private long cap;

        public MyBloomFilter(long cap) {
            this.cap = cap;
        }
        //Hash函数-->定义getOffset方法，获取位置信息
        public long getOffset(String value){
            long result = 0L;
            for (char c : value.toCharArray()) {
                result += result * 31 + c;
            }
            //取模
            return result & (cap - 1);
        }
    }

}
