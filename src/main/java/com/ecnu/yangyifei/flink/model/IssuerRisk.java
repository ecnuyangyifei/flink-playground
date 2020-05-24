package com.ecnu.yangyifei.flink.model;

import lombok.*;

import java.time.Instant;

@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IssuerRisk {
    public PositionLevelKey positionLevelKey;
    public Double jtd;
    public Instant timestamp;

}