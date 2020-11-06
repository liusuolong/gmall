package com.ym123.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author ymstart
 * @create 2020-11-06 11:05
 */
public interface DauMapper {
    //日活
    public Integer selectDauTotal(String date);

    //分时统计
    public List<Map> selectDauTotalHourMap(String date);

}
