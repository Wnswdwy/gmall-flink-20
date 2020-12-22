package com.wnswdwy.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author yycstart
 * @create 2020-12-19 14:15
*/

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ApacheLog {
    private String ip;
    private String userId;
    private Long eventTime;
    private String method;
    private String url;
}
