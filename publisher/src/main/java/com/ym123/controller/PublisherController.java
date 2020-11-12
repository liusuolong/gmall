package com.ym123.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import com.ym123.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author ymstart
 * @create 2020-11-06 13:43
 */
@RestController
public class PublisherController {

    @Autowired
    private PublisherService publisherService;
    //每日数据统计
    @RequestMapping("realtime-total")
    public String getDauTotal(@RequestParam("date") String date){

        //1.创建集合,存放结果
        ArrayList<Map> result = new ArrayList<>();
        //2.获取日活数据
        Integer dauTotal = publisherService.getDauTotal(date);
        //2.1获取GMV总数
        Double orderTotal = publisherService.getOrderTotal(date);

        //3.封装新增日活到Map
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);

        //4.封装新增设备到Map
        HashMap<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",233);

        //4.1GMV
        HashMap<String, Object> gmvMap= new HashMap<>();
        gmvMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",orderTotal);

        //5.将数据放入list
        result.add(dauMap);
        result.add(newMidMap);
        result.add(gmvMap);

        //将数组转换为字符串输出
        return JSON.toJSONString(result);
    }

    //分时统计处理
    @RequestMapping("realtime-hours")
    public String getDauTotalHourMap(@RequestParam("id") String id,
                                     @RequestParam("date") String date){
        //1.创建Map存放数据
        HashMap<String, Map> result = new HashMap<>();

        //2.获取昨天时间
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        //3.声明两个Map存放今天和昨天的数据
        Map todayMap = null;
        Map yesterdayMap = null;

        if("dau".equals(id)){
            //获取当日日活分时数据
            todayMap = publisherService.getDauTotalHourMap(date);
            //获取昨日日活分时数据
            yesterdayMap = publisherService.getDauTotalHourMap(yesterday);
        }else if("order_amount".equals(id)){
            //获取当天交易额分时数据
            todayMap = publisherService.getOrderTotalHourMap(date);
            //获取昨日交易额分时数据
            yesterdayMap = publisherService.getOrderTotalHourMap(yesterday);

        }

        //4.将两个Map放入result
        result.put("yesterday",yesterdayMap);
        result.put("today",todayMap);

        //5.返回结果
        return JSONObject.toJSONString(result);
    }

}
