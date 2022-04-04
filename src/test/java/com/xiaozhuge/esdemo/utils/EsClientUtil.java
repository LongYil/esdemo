package com.xiaozhuge.esdemo.utils;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * @author liyinlong
 * @description
 * @since 2022/4/4 18:10
 */
public class EsClientUtil {

    private static final String esHost = "192.168.31.143";

    public static RestHighLevelClient getEsClient(){
        // 创建es客户端
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost(esHost, 9200, "http")));

        return client;
    }

}
