package com.ym123.service.impl;

import com.ym123.mapper.DauMapper;
import com.ym123.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**业务查询实现类
 * @author ymstart
 * @create 2020-11-06 11:55
 */
@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private  DauMapper dauMapper;

    @Override
    //日活
    public Integer getDauTotal(String date) {

        return dauMapper.selectDauTotal(date);
    }

    //分时统计
    @Override
    public Map getDauTotalHourMap(String date) {
        //1.从phoenix中获取数据
        //List[Map]
        //Map:
        //[(LH->16),(CT->130)]
        //[(LH->17),(CT->346)]
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //2.创建map存储数据
        HashMap<String, Long> result = new HashMap<>();

        //3.遍历list
        //Map:
        //[(16->130),(17->346)]
        for (Map map : list) {
            result.put((String) map.get("LH"),(Long)map.get("CT"));
        }
        //返回数据
        return result;
    }



}
