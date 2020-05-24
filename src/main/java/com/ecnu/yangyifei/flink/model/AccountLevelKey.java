package com.ecnu.yangyifei.flink.model;

import lombok.*;
import lombok.experimental.SuperBuilder;

@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class AccountLevelKey {
    public Integer businessDate;
    public String smci;
    public String account;
}