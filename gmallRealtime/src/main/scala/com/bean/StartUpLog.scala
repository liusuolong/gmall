package com.bean

/**
 * @author ymstart
 * @create 2020-11-04 15:07
 */
case class StartUpLog(mid: String,
                      uid: String,
                      appid: String,
                      area: String,
                      os: String,
                      ch: String,
                      `type`: String,
                      vs: String,
                     //增加的两个字段
                      var logDate: String,
                      var logHour: String,
                      var ts: Long)

