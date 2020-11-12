package com.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.utils.MyKafkaSender;
import com.ym123.GmallConstants;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author ymstart
 * @create 2020-11-06 23:40
 */
public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //1.获取canal连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop002", 11111),
                "example",
                "",
                "");
        //2.kafka抓取数据并解析
        while (true) {
            //连接
            canalConnector.connect();
            //指定消费的数据表
            canalConnector.subscribe("gmall2020.*");
            //抓取数据
            Message message = canalConnector.get(100);
            //判断是否为空
            if (message.getEntries().size() <= 0) {
                System.out.println("暂时没数据！");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                //获取Entry集合
                List<CanalEntry.Entry> entries = message.getEntries();
                //遍历Entry集合
                for (CanalEntry.Entry entry : entries) {
                    //获取Entry类型
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    //判断 并只取EntryType中的ROWDATA
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                        //取表名
                        String tableName = entry.getHeader().getTableName();
                        //取序列化的数据
                        ByteString storeValue = entry.getStoreValue();
                        //反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        //获取EventType
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //获取RowDataList
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        //根据表名和事件类型处理数据rowDatasList
                        handler(tableName, eventType, rowDatasList);
                    }
                }
            }
        }

    }

    /**
     * 根据表名和事件类型处理数据rowDatasList
     *
     * @param tableName
     * @param eventType
     * @param rowDatasList
     */
    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            sendToKafka(rowDatasList, GmallConstants.GMALL_ORDER_INFO);

        } else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            sendToKafka(rowDatasList, GmallConstants.GMALL_ORDER_DETAIL);

        } else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType))) {
            sendToKafka(rowDatasList, GmallConstants.GMALL_USER_INFO);
        }
    }

    private static void sendToKafka(List<CanalEntry.RowData> rowDatasList, String topic) {
        for (CanalEntry.RowData rowData : rowDatasList) {
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                jsonObject.put(column.getName(), column.getValue());
            }
            System.out.println(jsonObject);
            //数据发往kafka
            MyKafkaSender.send(topic, jsonObject.toString());
        }
    }
}
