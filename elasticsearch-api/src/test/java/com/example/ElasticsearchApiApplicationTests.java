package com.example;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest
class ElasticsearchApiApplicationTests {

    @Autowired
    @Qualifier("getRestHighLevelClient")
    private RestHighLevelClient restHighLevelClient;
    @Test
    void contextLoads() {
    }


    /**
     * 测试索引的创建
     */
    @Test
    void createIndex() throws IOException {
        //1.创建索引请求
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("java-es");
        //2. 使用客户端执行索引请求
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        //3.获取创建索引请求的响应
        System.out.println(createIndexResponse);



    }
}
