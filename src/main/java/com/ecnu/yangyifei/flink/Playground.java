package com.ecnu.yangyifei.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


public class Playground {


    public static void main(String[] args) throws Exception {
        cabAnalytics(args);
    }


    private static void cabAnalytics(String[] args) throws Exception {

        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        boolean isCluster = params.has("input");
        String dataPath = "file:///Users/yifei/flink/demos/flink-palyground/data.csv";

        if (isCluster) {
            dataPath = params.get("input");
        }
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.readTextFile(dataPath);
        SingleOutputStreamOperator<Tuple8<String, String, String, String, String, String, String, Long>> cabData = lines.map(Playground::token);

        // most popular destination
//        cabData.map(Playground::destinationAndPassengers).keyBy(0).sum(1).map(Playground::fakeGroup).keyBy(0).maxBy(2).print();

        // avg num of passengers from each pickup location
//        cabData.map(Playground::pickupAndPassengers).keyBy(0).process(new KeyedProcessFunction<Tuple, Tuple2<String, Long>, Tuple2<String, Double>>() {
//
//            ValueState<Long> sum;
//            ValueState<Long> count;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                ValueStateDescriptor<Long> sumDesc = new ValueStateDescriptor<>("Sum", Long.class);
//                sum = getRuntimeContext().getState(sumDesc);
//                ValueStateDescriptor<Long> countDesc = new ValueStateDescriptor<>("Count", Long.class);
//                count = getRuntimeContext().getState(countDesc);
//            }
//
//            @Override
//            public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Double>> out) throws Exception {
//                if (null == sum.value() || count.value() == null) {
//                    sum.update(0L);
//                    count.update(0L);
//                }
//                sum.update(sum.value() + value.f1);
//                count.update(count.value() + 1);
//                out.collect(new Tuple2<>(value.f0, sum.value() * 1.0 / count.value()));
//            }
//        }).print();

        // avg number of trips for each driver
        cabData.filter(d -> d.f4.equals("Yes")).keyBy(d -> d.f3).process(new KeyedProcessFunction<String, Tuple8<String, String, String, String, String, String, String, Long>, Tuple2<String, Double>>() {
            ValueState<Long> sum;
            ValueState<Long> count;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Long> sumDesc = new ValueStateDescriptor<>("Sum", Long.class);
                sum = getRuntimeContext().getState(sumDesc);
                ValueStateDescriptor<Long> countDesc = new ValueStateDescriptor<>("Count", Long.class);
                count = getRuntimeContext().getState(countDesc);
            }

            @Override
            public void processElement(Tuple8<String, String, String, String, String, String, String, Long> value, Context ctx, Collector<Tuple2<String, Double>> out) throws Exception {
                if (null == sum.value() || count.value() == null) {
                    sum.update(0L);
                    count.update(0L);
                }
                sum.update(sum.value() + value.f7);
                count.update(count.value() + 1);
                out.collect(new Tuple2<>(value.f3, sum.value() * 1.0 / count.value()));
            }
        }).print();

        env.execute("Cab Analytics");
    }


    private static Tuple3<Integer, String, Long> fakeGroup(Tuple2<String, Long> keyBy) {
        return new Tuple3<Integer, String, Long>(1, keyBy.f0, keyBy.f1);
    }

    private static Tuple8<String, String, String, String, String, String, String, Long> token(String line) {
        String[] strings = line.split(",");
        return new Tuple8<>(strings[0], strings[1], strings[2], strings[3], strings[4], strings[5], strings[6], Long.parseLong(strings[7]));
    }

    private static Tuple2<String, Long> destinationAndPassengers(Tuple8 tuple8) {
        return new Tuple2(tuple8.f6, tuple8.f7);
    }

    private static Tuple2<String, Long> pickupAndPassengers(Tuple8 tuple8) {
        return new Tuple2(tuple8.f5, tuple8.f7);
    }


    private static class Accumulator {
        String key;
        Long count;
        Long sum;
    }


}
