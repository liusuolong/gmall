package com.ym123.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ym123.GmallConstants;
import lombok.extern.log4j.Log4j;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author ymstart
 * @create 2020-11-03 20:35
 */
//请求接收
@RestController
//@RestController = @Controller + @ResponseBody
@Controller
@Log4j
//@Slf4j
public class LoggerController {

    //private Logger logger = LoggerFactory.getLogger(LoggerController.class);

    @Autowired
    private KafkaTemplate kafkaTemplate;

    //映射
    @RequestMapping("test1")
    //返回对象
    @ResponseBody
    public String test01(){
        System.out.println("1");
        return "tttt";
    }

    @RequestMapping("test2")
    public String test02(@RequestParam("name") String name,@RequestParam("age") int age){
        System.out.println(name+":"+age);
        return "Q_Q";
    }

    @RequestMapping("log")
    public String Mocker(@RequestParam("logString") String lo){
        //logger.info(lo);
        //System.out.println(lo);
        //添加时间戳
        JSONObject jsonObject = JSON.parseObject(lo);
        jsonObject.put("ts",System.currentTimeMillis());
        //落盘
        String string = jsonObject.toJSONString();
        log.info(string);

        if ("startup".equals(jsonObject.getString("type"))){
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_START,string);
        }else{
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT,string);
        }
        log.info(lo);
        return "success";
    }
}
