package com.yisa.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.table.planner.expressions.In;

import java.math.BigInteger;

@Data
@NoArgsConstructor
public class FaceGroup {
    private BigInteger count;
    private BigInteger group;
    private BigInteger location_id;
    private Integer hour;
}
