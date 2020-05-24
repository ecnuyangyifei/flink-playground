package com.ecnu.yangyifei.flink;

import com.ecnu.yangyifei.flink.model.AccountLevelKey;
import com.ecnu.yangyifei.flink.model.AggregateIssuerRisk;
import com.ecnu.yangyifei.flink.model.IssuerRisk;
import com.ecnu.yangyifei.flink.model.PositionLevelKey;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

public class RTCAggDemo {


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<IssuerRisk> issuerRiskDataStreamSource = env.fromCollection(mockData());

        issuerRiskDataStreamSource
                .keyBy("positionLevelKey")
                .flatMap(new DeltaMap())
                .keyBy("positionLevelKey.accountLevelKey")
                .sum("jtd")
                .map(RTCAggDemo::toAggregated)
                .print();
        env.execute("Aggregate RTC");
    }


    static List<IssuerRisk> mockData() throws URISyntaxException, IOException {
        URL resource = RTCAggDemo.class.getResource("/IssuerRisk.csv");
        List<String> lines = Files.readAllLines(Paths.get(resource.toURI()));
        lines.remove(0);
        return lines.stream().map(RTCAggDemo::buildIssuerRisk).collect(Collectors.toList());
    }

    static IssuerRisk buildIssuerRisk(String line) {
        String[] split = line.split(",");
        AccountLevelKey accountLevelKey = AccountLevelKey.builder().businessDate(Integer.valueOf(split[0])).smci(split[1]).account(split[2]).build();

        PositionLevelKey key = PositionLevelKey.builder()
                .uid(split[3]).uidType(split[4])
                .accountLevelKey(accountLevelKey).build();
        return IssuerRisk.builder().positionLevelKey(key).jtd(Double.valueOf(split[5])).timestamp(Instant.parse(split[6])).build();
    }

    static AggregateIssuerRisk toAggregated(IssuerRisk issuerRisk) {
        return AggregateIssuerRisk.builder()
                .acountLevelKey(issuerRisk.positionLevelKey.accountLevelKey)
                .jtd(issuerRisk.jtd)
                .timestamp(Instant.now())
                .build();
    }


    static class DeltaMap extends RichFlatMapFunction<IssuerRisk, IssuerRisk> {
        private MapState<PositionLevelKey, Tuple2<Instant, Double>> positionJTDMap;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<PositionLevelKey, Tuple2<Instant, Double>> positionMapState = new MapStateDescriptor<>(
                    "positionMapState", TypeInformation.of(PositionLevelKey.class), TypeInformation.of(new TypeHint<Tuple2<Instant, Double>>() {
            }));
            this.positionJTDMap = getRuntimeContext().getMapState(positionMapState);
        }


        @Override
        public void flatMap(IssuerRisk value, Collector<IssuerRisk> out) throws Exception {
            if (positionJTDMap.contains(value.positionLevelKey)) {
                Tuple2<Instant, Double> existed = positionJTDMap.get(value.positionLevelKey);
                if (existed.f0.isBefore(value.timestamp)) {
                    positionJTDMap.put(value.positionLevelKey, new Tuple2<>(value.timestamp, value.jtd));
                    value.jtd -= existed.f1;
                    out.collect(value);
                }
            } else {
                positionJTDMap.put(value.positionLevelKey, new Tuple2<>(value.timestamp, value.jtd));
                out.collect(value);
            }
        }
    }


}
