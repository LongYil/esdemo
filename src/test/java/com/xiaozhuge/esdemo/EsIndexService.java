package com.xiaozhuge.esdemo;

import com.xiaozhuge.esdemo.utils.EsClientUtil;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;

/**
 * @author liyinlong
 * @description
 * @since 2022/4/4 18:58
 */
public class EsIndexService {

    RestHighLevelClient esClient = EsClientUtil.getEsClient();

    /**
     * 创建索引
     * @param indexName
     * @throws Exception
     */
    public void createIndex(String indexName) throws Exception{
        CreateIndexRequest goodsIndex = new CreateIndexRequest(indexName);
        CreateIndexResponse response = esClient.indices().create(goodsIndex, RequestOptions.DEFAULT);
        boolean acknowledged = response.isAcknowledged();
        System.out.println("索引创建结果：" + acknowledged);
    }

    /**
     * 查询索引信息
     * @param indexName
     * @throws Exception
     */
    public void queryIndex(String indexName) throws Exception{
        GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
        GetIndexResponse response = esClient.indices().get(getIndexRequest, RequestOptions.DEFAULT);
        System.out.println(response.getAliases());
        System.out.println(response.getMappings());
        System.out.println(response.getSettings());
    }

    /**
     * 删除索引
     * @param indexName
     * @throws Exception
     */
    public void deleteIndex(String indexName) throws Exception{
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
        AcknowledgedResponse response = esClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println("索引删除结果：" + response.isAcknowledged());
    }
}
