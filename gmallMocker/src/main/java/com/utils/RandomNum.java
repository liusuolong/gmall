package com.utils;

import java.util.Random;

/**
 * @author ymstart
 * @create 2020-11-03 19:06
 */
public class RandomNum {
    public static int getRandInt(int fromNum,int toNum){
        return fromNum + new Random().nextInt(toNum-fromNum+1);
    }
}

