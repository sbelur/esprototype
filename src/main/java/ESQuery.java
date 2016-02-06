import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;

/**
 * Created by sbelur on 07/02/16.
 */
public class ESQuery {

  private TransportClient transportClient;

  public ESQuery(){
    Settings settings =  ImmutableSettings.settingsBuilder()
        .put("http.port", 9200)
        .put("cluster.name", "cleo.elasticsearch").build();

    transportClient =new TransportClient(settings);
    transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));


  }


  public SearchResponse searchResultWithAggregation(
      ) {

    QueryBuilder qb = QueryBuilders.boolQuery()
        .mustNot(QueryBuilders.termQuery("type", "detail"))
        .mustNot(QueryBuilders.termQuery("type", "end"))
        .mustNot(QueryBuilders.termQuery("type", "request"))
        .mustNot(QueryBuilders.termQuery("type", "response"))
        .mustNot(QueryBuilders.termQuery("type", "transfer"))
        .mustNot(QueryBuilders.termQuery("type", "command"));
        //.should(QueryBuilders.rangeQuery("date").gte("2016-02-06T04:12:51.255+0530").lte("2016-02-07T04:12:51.255+0530");

    //queryRangeTime = "now-" + queryRangeTime + "m";

    FilterBuilder fb = FilterBuilders.rangeFilter("date")
        .gte("2016-02-06T04:12:51.255+0530").lte("2016-02-07T04:12:51.255+0530");

    SearchResponse response = transportClient.prepareSearch("versalex-2016-02-06")
        .setTypes("systemlog")
        .setQuery(qb)
        .setPostFilter(fb)
        .setSize(10).execute().actionGet();

    SearchHit hits[] = response.getHits().hits();
    Arrays.stream(hits).forEach(h -> System.out.println("A hit "  +h.getSourceAsString()));

//    System.out.println(response.toString());
    return response;
  }

  public static void main(String[] args) {
    new ESQuery().searchResultWithAggregation();
  }

}
