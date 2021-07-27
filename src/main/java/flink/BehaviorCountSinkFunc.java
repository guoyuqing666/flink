package flink;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

public class BehaviorCountSinkFunc implements ElasticsearchSinkFunction<Object []> {

    private String indexName = "";

    public BehaviorCountSinkFunc(String indexName) {
        this.indexName = indexName;
    }

    public IndexRequest createIndexRequest(Object [] element) {
        Map<String, Object> json = new HashMap<>();
        json.put("Buy", element[0]);
        json.put("Fav", element[1]);
        json.put("Cart", element[2]);
        json.put("Pv", element[3]);
        json.put("Time", element[4]);
        System.out.println(indexName + " 发送数据：" + json.toString());
        

        return Requests.indexRequest().index(indexName).source(json);
    }
    
    @Override
    public void process(Object[] element, RuntimeContext ctx, RequestIndexer indexer) {
        indexer.add(createIndexRequest(element));
    }
}