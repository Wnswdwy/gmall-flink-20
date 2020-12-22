package com.wnswdwy.app;

import com.wnswdwy.bean.LoginEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * @author yycstart
 * @create 2020-12-22 10:29
 */
public class LoginFailwithStateCEP_More {
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

        //3. 按照用户id分组
        KeyedStream<LoginEvent, Long> keyedStream = loginEventDS.keyBy(LoginEvent::getUserId);

        //4. 定义模式序列两次失败
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent) throws Exception {
                return "fail".equals(loginEvent.getEventType());
            }
        }).times(2) //默认使用的是非严格近邻
          .consecutive()//指定严格紧邻
          .within(Time.seconds(2));


        //5. 将模式系列作用到流上
        PatternStream<LoginEvent> patternStream = CEP.pattern(keyedStream, pattern);

        //6. 提取事件
        SingleOutputStreamOperator<String> result = patternStream.select(new MyPatternSelectFunc());

        //7. 打印
        result.print();

        //8. 执行
        env.execute();

    }
    public static class MyPatternSelectFunc implements PatternSelectFunction<LoginEvent, String> {
        @Override
        public String select(Map<String, List<LoginEvent>> map) throws Exception {
            List<LoginEvent> events = map.get("start");
            LoginEvent start = events.get(0);
            LoginEvent next = events.get(events.size() - 1);
            return start.getUserId() + "在" + start.getTimestamp() + "到" + next.getTimestamp() + "连续登陆失败两次";
        }
    }
}
