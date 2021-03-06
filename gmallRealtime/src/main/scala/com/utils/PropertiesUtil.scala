package com.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * @author ymstart
 * @create 2020-11-04 14:33
 */
object PropertiesUtil {

  def load(propertieName:String): Properties ={
    val prop=new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }
}

