package com.ym123;

/**
 * @author ymstart
 * @create 2020-11-04 11:14
 */
public class GmallConstants {
    //启动日志主题
    public static final String KAFKA_TOPIC_START ="GMALLSTARTUP";

    //事件日志主题
    public static final String KAFKA_TOPIC_EVENT ="GMALLEVENT";

    //订单日志主题
    public static final String GMALL_ORDER_INFO ="GMALL_ORDER_INFO";

    //订单详情日志主题
    public static final String GMALL_ORDER_DETAIL ="GMALL_ORDER_DETAIL";

    //用户信息日志主题
    public static final String GMALL_USER_INFO ="GMALL_USER_INFO";

    //预警日志ES Index前缀
    public static final String ES_ALERT_INDEX_PRE = "gmall_coupon_alert";
}