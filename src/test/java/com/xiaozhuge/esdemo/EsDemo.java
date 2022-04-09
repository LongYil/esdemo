package com.xiaozhuge.esdemo;

import com.xiaozhuge.esdemo.bean.User;
import com.xiaozhuge.esdemo.utils.EsClientUtil;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;

import java.io.IOException;
import java.util.Map;

/**
 * @author liyinlong
 * @description
 * @since 2022/4/4 17:49
 */
public class EsDemo {

    RestHighLevelClient esClient = EsClientUtil.getEsClient();

    public static void main(String[] args) throws Exception {
        EsDocumentService service = new EsDocumentService();
        //indexService.createIndex("hcuser");
        User user = new User();
        user.setName("测试1");
        user.setPassword("123");
        user.setAge(24);
        //documentService.create("hcuser", user, "1002");
        //documentService.update("hcuser", "1002");
        //service.getIndices();

        //service.getByIndex("hcuser", "1002");

        //service.deleteByIndex("hcuser", "1002");

        //service.batchCreate("hcuser");

        //service.batchDelete("hcuser");

//        service.batchCreate("hcuser");

//        service.matchAllQuery("hcuser");

        //service.termQuery("hcuser");

//        service.highLevelQuery("hcuser");

//        service.multiQuery("shopping");

//        service.rangeQuery("shopping");

//        service.fuzzyQuery("shopping");

//        service.highLightQuery("shopping");

//        service.aggrQuery("shopping");

        service.groupQuery("shopping");

    }
    
}
