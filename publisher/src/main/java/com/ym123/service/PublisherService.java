package com.ym123.service;

import java.util.Map;

/**数据查询业务
 * @author ymstart
 * @create 2020-11-06 11:52
 */
public interface PublisherService {

    //日活
    public Integer getDauTotal(String date);

    //分时统计
    public Map getDauTotalHourMap(String date);
}
