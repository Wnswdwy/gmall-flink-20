package com.wnswdwy.bean;

/**
 * @author yycstart
 * @create 2020-12-22 14:07
 */
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderEvent {
    private Long orderId;
    private String eventType;
    private String txId;
    private Long eventTime;
}
