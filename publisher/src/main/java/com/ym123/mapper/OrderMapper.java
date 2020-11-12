package com.ym123.mapper;

import java.util.List;
import java.util.Map;

/**GMV
 * @author ymstart
 * @create 2020-11-12 19:54
 */
public interface OrderMapper {
    //1.当日交易总额
    public  Double selectOrderAmountTotal(String date);
    //2.交易额分时明细
    public List<Map> selectOrderAmountHourMap(String date);
}
