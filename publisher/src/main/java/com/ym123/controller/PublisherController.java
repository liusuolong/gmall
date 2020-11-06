package com.ym123.controller;

import com.alibaba.fastjson.JSON;
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
    //日活数据处理
    @RequestMapping("realtime-total")
    public String getDauTotal(@RequestParam("date") String date){
        //1.创建集合,存放结果
        ArrayList<Map> result = new ArrayList<>();
        //2.获取日活数据
        Integer dauTotal = publisherService.getDauTotal(date);
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

        //5.将数据放入list
        result.add(dauMap);
        result.add(newMidMap);

        //将数组转换为字符串输出
        return JSON.toJSONString(result);
    }

    //分时统计处理
    @RequestMapping("realtime-hours")
    public String getDauTotalHourMap(@RequestParam("id") String id,
                                     @RequestParam("date") String date){
        //1.获取当天的数据
        Map todayMap = publisherService.getDauTotalHourMap(date);
        //2.获取昨天的数据
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();
        Map yesterdayTotalMap = publisherService.getDauTotalHourMap(yesterday);

        //3封装到集合中
        HashMap<String,Map> result = new HashMap<>();
        result.put("yesterday",yesterdayTotalMap);
        result.put("today",todayMap);
        //4.返回数据
        return JSON.toJSONString(result);

    }

}
