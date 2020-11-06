package com.ym123;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
//包扫描
@MapperScan(basePackages = "com.ym123.mapper")
public class PublisherApplication {
	public static void main(String[] args) {
		SpringApplication.run(PublisherApplication.class, args);
	}

}
