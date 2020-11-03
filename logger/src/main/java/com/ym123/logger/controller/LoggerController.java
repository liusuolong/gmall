package com.ym123.logger.controller;

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
public class LoggerController {
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
        System.out.println(lo);
        return "success";
    }
}
