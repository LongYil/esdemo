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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * @author liyinlong
 * @description
 * @since 2022/4/4 18:59
 */
public class EsDocumentService {

    RestHighLevelClient esClient = EsClientUtil.getEsClient();

    /**
     * ????????????
     * @param indexName
     * @param user
     * @param id
     * @throws Exception
     */
    public void create(String indexName, User user,String id) throws Exception{
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index(indexName).id(id);

        //???es????????????????????????????????????json??????
        ObjectMapper mapper = new ObjectMapper();
        String userJsonStr = mapper.writeValueAsString(user);

        indexRequest.source(userJsonStr, XContentType.JSON);

        IndexResponse response = esClient.index(indexRequest, RequestOptions.DEFAULT);

        System.out.println(response.getResult());
    }

    /**
     * ??????????????????
     * @param indexName
     * @throws Exception
     */
    public void batchCreate(String indexName) throws Exception{
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest().index(indexName).id("12").source(XContentType.JSON, "name","ceshi1", "password","123", "age", 23));
        request.add(new IndexRequest().index(indexName).id("23").source(XContentType.JSON, "name","ceshi2", "password","123", "age", 62));
        request.add(new IndexRequest().index(indexName).id("34").source(XContentType.JSON, "name","ceshi3", "password","123", "age", 16));
        BulkResponse response = esClient.bulk(request, RequestOptions.DEFAULT);
        System.out.println(response.getTook());
        System.out.println(response.getItems());
    }


    /**
     * ??????
     * @param indexName
     * @param id
     * @throws Exception
     */
    public void update(String indexName,String id) throws Exception{
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(indexName).id(id);
        updateRequest.doc(XContentType.JSON, "name","??????2");
        UpdateResponse response = esClient.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(response.getResult());
    }

    /**
     * ??????????????????
     */
    public void getIndices(){
        GetRequest request = new GetRequest();
        String[] indices = request.indices();
        System.out.println(Arrays.toString(indices));
    }

    /**
     * ????????????
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
     * ????????????
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
     * ????????????
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

    /**
     * ???????????????????????????
     * @param indexName
     * @throws Exception
     */
    public void matchAllQuery(String indexName) throws Exception{
        SearchRequest request = new SearchRequest();
        //??????????????????????????????
        request.indices(indexName);
        //????????????????????????
        SearchSourceBuilder query = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        request.source(query);

        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
        System.out.println(response.getTook());
    }

    /**
     * ????????????
     * @param indexName
     * @throws Exception
     */
    public void termQuery(String indexName) throws Exception{
        SearchRequest request = new SearchRequest();
        //??????????????????????????????
        request.indices(indexName);
        //????????????????????????
        SearchSourceBuilder query = new SearchSourceBuilder().query(QueryBuilders.termQuery("name","ceshi3"));
        request.source(query);

        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
        System.out.println(response.getTook());
    }

    /**
     * ????????????
     * @param indexName
     * @throws Exception
     */
    public void highLevelQuery(String indexName) throws Exception{
        SearchRequest request = new SearchRequest();
        //??????????????????????????????
        request.indices(indexName);
        //????????????????????????
        SearchSourceBuilder query = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        //???????????? from = (page-1) * size
        query.from(0);
        query.size(2);
        //??????
        query.sort("age", SortOrder.DESC);
        //???????????????????????????
        String[] includes = new String[]{"name"};
        String[] excludes = new String[]{"age"};

        query.fetchSource(includes, excludes);

        request.source(query);

        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
        System.out.println(response.getTook());
    }

    /**
     * ????????????
     * @param indexName
     */
    public void multiQuery(String indexName) throws Exception{
        SearchRequest request = new SearchRequest();
        request.indices(indexName);

        SearchSourceBuilder builder = new SearchSourceBuilder();

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
//        boolQuery.must(QueryBuilders.matchQuery("category", "??????"));
//        boolQuery.mustNot(QueryBuilders.matchQuery("price", "6888"));

        boolQuery.should(QueryBuilders.matchQuery("price", "6888"));
        boolQuery.should(QueryBuilders.matchQuery("price", "5888"));

        builder.query(boolQuery);

        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * ????????????
     * @param indexName
     * @throws Exception
     */
    public void rangeQuery(String indexName) throws Exception{
        SearchRequest request = new SearchRequest();
        request.indices(indexName);

        SearchSourceBuilder builder = new SearchSourceBuilder();

        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("price");
        //??????2000-5000???????????????
//        rangeQuery.from(2000);
//        rangeQuery.to(5000);

        //????????????3000???????????????5000?????????
        rangeQuery.lte(4888);
        rangeQuery.gt(3000);

        builder.query(rangeQuery);

        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * ????????????
     * @param indexName
     * @throws Exception
     */
    public void fuzzyQuery(String indexName) throws Exception{
        SearchRequest request = new SearchRequest();
        request.indices(indexName);

        SearchSourceBuilder builder = new SearchSourceBuilder();
        FuzzyQueryBuilder queryBuilder = QueryBuilders.fuzzyQuery("title", "??????p50").fuzziness(Fuzziness.TWO);
        builder.query(queryBuilder);

        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * ????????????????????????
     * @param indexName
     */
    public void highLightQuery(String indexName) throws Exception{
        SearchRequest request = new SearchRequest();
        //??????????????????????????????
        request.indices(indexName);
        //????????????????????????
        SearchSourceBuilder builder = new SearchSourceBuilder();

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<font color='red'>");
        highlightBuilder.postTags("</font>");
        highlightBuilder.field("title");

        builder.highlighter(highlightBuilder);
        builder.query(QueryBuilders.matchAllQuery());

        request.source(builder);

        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        System.out.println(response.getHits().getTotalHits());
        for (SearchHit hit : hits.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
        System.out.println(response.getTook());
    }

    public void aggrQuery(String indexName) throws Exception{
        SearchRequest request = new SearchRequest();
        //??????????????????????????????
        request.indices(indexName);
        //????????????????????????
        SearchSourceBuilder builder = new SearchSourceBuilder();

        AggregationBuilder aggregationBuilder = AggregationBuilders.max("maxPrice").field("price");

        builder.aggregation(aggregationBuilder);

        builder.query(QueryBuilders.matchAllQuery());

        request.source(builder);

        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        System.out.println(hits.getTotalHits());
        System.out.println(response.getTook());
        for (SearchHit hit : hits.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
        System.out.println(response.getTook());

    }

    public void groupQuery(String indexName) throws Exception{
        SearchRequest request = new SearchRequest();
        //??????????????????????????????
        request.indices(indexName);
        //????????????????????????
        SearchSourceBuilder builder = new SearchSourceBuilder();

        AggregationBuilder aggregationBuilder = AggregationBuilders.terms("categoryGroup").field("price");

        builder.aggregation(aggregationBuilder);

        builder.query(QueryBuilders.matchAllQuery());

        request.source(builder);

        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        Aggregations aggregations = response.getAggregations();

        System.out.println("????????????????????????");
        Map<String, Aggregation> map = aggregations.getAsMap();

        Aggregation categoryGroup = map.get("categoryGroup");
        System.out.println(categoryGroup.getType());


        SearchHits hits = response.getHits();
        System.out.println(hits.getTotalHits());
        System.out.println(response.getTook());
        for (SearchHit hit : hits.getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        System.out.println(response.getTook());
    }

}
