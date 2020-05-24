package com.ecnu.yangyifei.flink.model;

import lombok.*;

@ToString
@NoArgsConstructor
@AllArgsConstructor
@Builder
@EqualsAndHashCode
public class PositionLevelKey {
    public AccountLevelKey accountLevelKey;
    public String uid;
    public String uidType;
}