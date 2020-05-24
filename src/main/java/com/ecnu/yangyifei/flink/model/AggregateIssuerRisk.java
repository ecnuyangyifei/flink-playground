package com.ecnu.yangyifei.flink.model;

import lombok.*;

import java.time.Instant;

@ToString
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AggregateIssuerRisk {
    public AccountLevelKey acountLevelKey;
    public Double jtd;
    public Instant timestamp;
}