package com.ym123.logger.controller;

import lombok.extern.log4j.Log4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class LoggerController {

    //private Logger logger = LoggerFactory.getLogger(LoggerController.class);

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
        log.info(lo);
        return "success";
    }
}
