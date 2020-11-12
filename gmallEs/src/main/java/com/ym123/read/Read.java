package com.ym123.read;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * @author ymstart
 * @create 2020-11-09 19:55
 */
public class Read {
    public static void main(String[] args) throws IOException {
        //1.获取连接
        JestClientFactory jestClientFactory = new JestClientFactory();

        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop002:9200").build();

        jestClientFactory.setHttpClientConfig(httpClientConfig);

        JestClient jestClient = jestClientFactory.getObject();

        //2.查询数据
        //方式一:
        Search index = new Search.Builder("GET student3/_search\n" +
                "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"username\": \"男孩子\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"must\": [\n" +
                "        {\n" +
                "          \"match\": {\n" +
                "            \"birth\": \"1997\"\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"groupByFavor2\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"favor2\",\n" +
                "        \"size\": 10\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}")
                .addIndex("student3")
                .addType("_doc")
                .build();
        //方式二:
        /*SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //2.1查询数据条件
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        new TermQueryBuilder()

        searchSourceBuilder.query(boolQueryBuilder)*/

        //3.解析
        JestResult result = jestClient.execute(index);
        //3.1获取总数

    }
}
