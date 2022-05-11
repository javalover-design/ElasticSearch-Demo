package com.example;

import com.alibaba.fastjson.JSON;
import com.example.pojo.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

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

    /**
     * 获取索引
     */
    @Test
    void getIndex() throws IOException {
        //1.获取索引请求
        GetIndexRequest getIndexRequest = new GetIndexRequest("java-es");
        //2.使用客户端判断索引是否存在
        boolean exists = restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        //3获取判断结果
        System.out.println(exists);
    }

    /**
     * 删除索引
     * @throws IOException
     */
    @Test
    void deleteIndex() throws IOException {
        //1.创建删除索引请求
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("java-es");
        //2.使用客户端的删除方法删除
        AcknowledgedResponse delete = restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        //3.最后获取删除的结果
        System.out.println(delete.isAcknowledged());
    }


    /**
     *测试添加文档
     */
    @Test
    void createDocument() throws IOException {
        //创建需要添加的实体类对象
        User user = new User("Jack", 16);
        //创建一个索引请求，用于添加数据的
        IndexRequest indexRequest = new IndexRequest("java-es");
        //为请求添加内容，比如id，超时时间、实体类对象存入
        indexRequest.id("1");
        indexRequest.timeout("10s");
        //将user对象转换成JSON字符串
        indexRequest.source(JSON.toJSONString(user), XContentType.JSON);

        //使用客户端发送请求。得到响应的结果
        IndexResponse index = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(index.status());
        System.out.println(index.toString());
    }

    /**
     *判断文档是否存在
     */
    @Test
    void testGetDocument() throws IOException {
        //创建获取文档的请求对象
        GetRequest getRequest = new GetRequest("java-es","1");
        //对获取的请求对象过滤一些source
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        //客户端调用exists方法判断文档存不存在
        boolean exists = restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     *获取文档的具体内容
     * @throws IOException
     */
    @Test
    void testGetDocumentContent() throws IOException {
        //1.首先仍旧是创建一个获取请求对象
        GetRequest getRequest = new GetRequest("java-es", "1");
        //2.使用客户端的get方法获取文档的内容响应对象
        GetResponse documentFields = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        //3.之后对响应对象进行内容的获取
        System.out.println(documentFields.getSourceAsString());
        System.out.println(documentFields.getId());
        System.out.println(documentFields.getVersion());
        System.out.println(documentFields.getIndex());
        System.out.println(documentFields);
    }

    /**
     *更新文档内容
     * @throws IOException
     */
    @Test
    void testUpdateDocumentContent() throws IOException {
        //1.创建一个更新请求对象，指定要更新的条件
        UpdateRequest updateRequest = new UpdateRequest("java-es", "1");
        //2.设置请求超时时间
        updateRequest.timeout("2s");
        //3.设置要更新的实体类对象
        User user = new User("Mike", 19);
        //4.以文档的形式更新
        updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);
        //5.最后得到更新的结果
        UpdateResponse update = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(update.status());
        System.out.println(update);
    }

    /**
     * 测试删除文档数据
     * @throws IOException
     */
    @Test
    void testDeleteDocument() throws IOException {
        //1.创建一个删除请求对象
        DeleteRequest deleteRequest = new DeleteRequest("java-es", "1");
        //2.设置超时时间为2s
        deleteRequest.timeout("2s");
        //3.执行删除请求返回删除响应
        DeleteResponse delete = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        //4.获取响应信息
        System.out.println(delete.status());
        System.out.println(delete);
    }


    /**
     * 批处理操作测试
     * @throws IOException
     */
    @Test
    void testBulkRequest() throws IOException {
        //1.创建批处理操作请求
        BulkRequest bulkRequest = new BulkRequest();
        //2.设置批处理请求的过期时间
        bulkRequest.timeout("10s");

        //3.创建内容集合对象
        List<User> userList = new ArrayList<>();
        userList.add(new User("Jack",15));
        userList.add(new User("Alan",19));
        userList.add(new User("Mike",20));
        userList.add(new User("Bruce",23));

        //4. 批处理请求，对遍历的每一个对象进行相应的操作，IndexRequest表示插入内容
        for (int i = 0; i < userList.size(); i++) {
            bulkRequest.add(new IndexRequest("java-es")
                    .id(""+(i+1))
                    .source(JSON.toJSONString(userList.get(i)),XContentType.JSON));
        }

        //5.最后获取插入后的响应对象
        BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        //6.对响应对象进行查看
        System.out.println(bulk);
        System.out.println(bulk.status());
        System.out.println(bulk.hasFailures());


    }

    /**
     * 对索引中的内容进行相应查询
     * @throws IOException
     */
    @Test
    void testSearch() throws IOException {
        //创建一个搜索请求
        SearchRequest searchRequest = new SearchRequest("java-es");
        //创建一个搜索资源建造者
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //创造精确查询构造者
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name.keyword", "Jack");
        //搜索资源建造者执行term精确查询
        searchSourceBuilder.query(termQueryBuilder);
        //searchSourceBuilder.from();
        //searchSourceBuilder.size();
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        //搜索请求存入搜索资源建造者信息
         searchRequest.source(searchSourceBuilder);
         //最后通过客户端执行搜索请求返回搜索响应对象
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(search.getHits()));
        System.out.println(search.toString());
        System.out.println(search.status());
        System.out.println("===========");
        //最后对于命中的每个对象都进行遍历
        for (SearchHit hit : search.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());

        }
    }
}
