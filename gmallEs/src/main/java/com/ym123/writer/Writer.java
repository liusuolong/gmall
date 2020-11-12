package com.ym123.writer;

import com.ym123.bean.WriterBean;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;


/**
 * @author ymstart
 * @create 2020-11-09 15:14
 */
public class Writer {

    public static void main(String[] args) throws IOException {
        //1.创建ES客户端构建器
        JestClientFactory jestClientFactory = new JestClientFactory();
        //2.创建ES客户端连接地址
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop002:9200").build();
        //3.设置ES连接地址
        jestClientFactory.setHttpClientConfig(httpClientConfig);
        //4.ES客户端连接
        JestClient jestClient = jestClientFactory.getObject();
        //5.构建es插入数据对象
        //方式一:
        /*Index index = new Index.Builder("PUT student3/_doc/0004\n" +
                "{\n" +
                "  \"username\":\"孩子王\",\n" +
                "  \"stu_id\":1000,\n" +
                "  \"birth\":\"1997-01-01\",\n" +
                "  \"gender\":\"男\",\n" +
                "  \"favor1\":\"学习篮球吉他\",\n" +
                "  \"favor2\":\"敲代码\"\n" +
                "}")
                .index("student3")
                .type("_doc")
                .build();*/
        //方式二:
        WriterBean writerBean = new WriterBean("孩子王", 1004,
                "1995-01-01", "男",
                "学习篮球吉他", "敲代码");
        Index index = new Index.Builder(writerBean)
                .index("student3")
                .type("_doc")
                .id("1005")
                .build();
        //6.执行插入操作
        jestClient.execute(index);
        jestClient.shutdownClient();
    }

}
