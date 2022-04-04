package com.xiaozhuge.esdemo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xiaozhuge.esdemo.bean.User;
import com.xiaozhuge.esdemo.utils.EsClientUtil;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Arrays;

/**
 * @author liyinlong
 * @description
 * @since 2022/4/4 18:59
 */
public class EsDocumentService {

    RestHighLevelClient esClient = EsClientUtil.getEsClient();

    /**
     * 创建文档
     * @param indexName
     * @param user
     * @param id
     * @throws Exception
     */
    public void create(String indexName, User user,String id) throws Exception{
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index(indexName).id(id);

        //向es插入数据，必须将数据转为json格式
        ObjectMapper mapper = new ObjectMapper();
        String userJsonStr = mapper.writeValueAsString(user);

        indexRequest.source(userJsonStr, XContentType.JSON);

        IndexResponse response = esClient.index(indexRequest, RequestOptions.DEFAULT);

        System.out.println(response.getResult());
    }

    /**
     * 批量插入数据
     * @param indexName
     * @throws Exception
     */
    public void batchCreate(String indexName) throws Exception{
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest().index(indexName).id("12").source(XContentType.JSON, "name","ceshi1"));
        request.add(new IndexRequest().index(indexName).id("23").source(XContentType.JSON, "name","ceshi2"));
        request.add(new IndexRequest().index(indexName).id("34").source(XContentType.JSON, "name","ceshi3"));
        BulkResponse response = esClient.bulk(request, RequestOptions.DEFAULT);
        System.out.println(response.getTook());
        System.out.println(response.getItems());
    }


    /**
     * 更新
     * @param indexName
     * @param id
     * @throws Exception
     */
    public void update(String indexName,String id) throws Exception{
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(indexName).id(id);
        updateRequest.doc(XContentType.JSON, "name","测试2");
        UpdateResponse response = esClient.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(response.getResult());
    }

    /**
     * 查询全部索引
     */
    public void getIndices(){
        GetRequest request = new GetRequest();
        String[] indices = request.indices();
        System.out.println(Arrays.toString(indices));
    }

    /**
     * 查询文档
     * @param indexName
     * @param id
     * @throws Exception
     */
    public void getByIndex(String indexName,String id) throws Exception{
        GetRequest request = new GetRequest();
        request.index(indexName).id(id);
        GetResponse response = esClient.get(request, RequestOptions.DEFAULT);
        String source = response.getSourceAsString();
        System.out.println(source);
    }

    /**
     * 删除文档
     * @param indexName
     * @param id
     * @throws Exception
     */
    public void deleteByIndex(String indexName,String id) throws Exception{
        DeleteRequest request = new DeleteRequest();
        request.index(indexName).id(id);
        DeleteResponse response = esClient.delete(request, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    /**
     * 批量删除
     * @param indexName
     * @throws Exception
     */
    public void batchDelete(String indexName) throws Exception{
        BulkRequest request = new BulkRequest();
        request.add(new DeleteRequest().index(indexName).id("12"));
        request.add(new DeleteRequest().index(indexName).id("23"));
        request.add(new DeleteRequest().index(indexName).id("34"));
        BulkResponse response = esClient.bulk(request, RequestOptions.DEFAULT);
        System.out.println(response.getTook());
        System.out.println(response.getItems());
    }

}
