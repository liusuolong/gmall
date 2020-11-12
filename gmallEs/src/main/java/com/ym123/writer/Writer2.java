package com.ym123.writer;

import com.ym123.bean.WriterBean;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

/**
 * @author ymstart
 * @create 2020-11-09 19:42
 */
public class Writer2 {
    public static void main(String[] args) throws IOException {
        //1.创建ES客户端构建器
        JestClientFactory jestClientFactory = new JestClientFactory();
        //2.创建ES客户端连接地址
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop002:9200").build();
        //3.设置ES连接地址
        jestClientFactory.setHttpClientConfig(httpClientConfig);
        //4.ES客户端连接
        JestClient esClient = jestClientFactory.getObject();
        //5.构建es插入数据对象
        WriterBean writerBean = new WriterBean("胖虎", 1006, "1994-12-12", "男", "棒球", "揍大雄");
        Index index = new Index.Builder(writerBean).index("student3").type("_doc").id("1001").build();
        //6.执行插入操作
        esClient.execute(index);
        //7.关闭
        esClient.shutdownClient();
    }
}
