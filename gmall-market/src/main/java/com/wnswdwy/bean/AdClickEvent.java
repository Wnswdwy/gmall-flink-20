package com.wnswdwy.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author yycstart
 * @create 2020-12-21 14:26
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AdClickEvent {
    private Long userId;
    private Long adId;
    private String province;
    private String city;
    private Long timestamp;
}

