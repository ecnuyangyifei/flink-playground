package com.ecnu.yangyifei.flink;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;


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

        SingleOutputStreamOperator<Tuple8<String, String, String, String, String, String, String, Long>> onTrip = cabData.filter(d -> d.f4.equals("Yes"));

        // most popular destination
        SingleOutputStreamOperator<Tuple2<String, Long>> maxBy =
                onTrip.map(Playground::destinationAndPassengers).keyBy(0).reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                }).keyBy(0).maxBy(1)
//                        .window(GlobalWindows.create()).trigger(CountTrigger.of(1)).sum(1).windowAll(GlobalWindows.create())
//                .trigger(CountTrigger.of(1)).maxBy(1)
                ;
        maxBy.print();

        // avg passengers cnt
//        ReduceFunction<Tuple2<String, Long>> tuple2ReduceFunction = (value1, value2) -> new Tuple2<String, Long>(value1.f0, (value1.f1 + value2.f1) / 2);
//        SingleOutputStreamOperator<Tuple2<String, Long>> avg = cabData.map(Playground::pickupAndPassengers).keyBy(d -> d.f0).reduce(tuple2ReduceFunction);
//
////        cabData.map(Playground::pickupAndPassengers).keyBy(0).ap
//        avg.print();

//        SingleOutputStreamOperator<Tuple8<String, String, String, String, String, String, String, Long>> passengersSumByPerson = cabData
//                .keyBy(0, 1, 2, 3).sum(7)
//                ;
//        if (isCluster) {
//            final StreamingFileSink<Tuple8<String, String, String, String, String, String, String, Long>> sink = StreamingFileSink.forRowFormat(new Path("file:///opt/flink/res"), new SimpleStringEncoder<Tuple8<String, String, String, String, String, String, String, Long>>("UTF-8")).build();
//            passengersSumByPerson.addSink(sink);
//        }
//        passengersSumByPerson.print();
        env.execute("Cab Analytics");
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


    private static class AvgAccumulator {
        Long count;
        Long sumOfPassengers;
    }

    private static class AvgPassengerCnt implements AggregateFunction<Long, AvgAccumulator, Double> {

        @Override
        public AvgAccumulator createAccumulator() {
            return new AvgAccumulator();
        }

        @Override
        public AvgAccumulator add(Long value, AvgAccumulator accumulator) {
            AvgAccumulator avgAccumulator = new AvgAccumulator();
            avgAccumulator.count = accumulator.count + 1;
            avgAccumulator.sumOfPassengers = accumulator.sumOfPassengers + value;
            return avgAccumulator;
        }

        @Override
        public Double getResult(AvgAccumulator accumulator) {
            return accumulator.sumOfPassengers * 1.0 / accumulator.count;
        }

        @Override
        public AvgAccumulator merge(AvgAccumulator a, AvgAccumulator b) {
            AvgAccumulator avgAccumulator = new AvgAccumulator();
            avgAccumulator.sumOfPassengers = a.sumOfPassengers + b.sumOfPassengers;
            avgAccumulator.count = a.count + b.count;
            return avgAccumulator;
        }
    }
}
