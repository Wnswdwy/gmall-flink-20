package com.wnswdwy.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author yycstart
 * @create 2020-12-19 14:13
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UrlCount {
     private String url;
     private Long windowEnd;
     private Long count;
}
