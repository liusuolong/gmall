package com.ym123.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * @author ymstart
 * @create 2020-11-09 19:27
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class WriterBean {
    private String username;
    private int stu_id;
    private String birth;
    private String gender;
    private String favor1;
    private String favor2;
}
