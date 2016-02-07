import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;

/**
 * Created by sbelur on 07/02/16.
 */
public class ESQuery {

  private TransportClient transportClient;

  public ESQuery() {
    Settings settings = ImmutableSettings.settingsBuilder().put("http.port", 9200)
        .put("cluster.name", "cleo.elasticsearch").build();

    transportClient = new TransportClient(settings);
    transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

  }

  public SearchResponse searchResultWithAggregation() {

    QueryBuilder qb = QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("type", "detail"))
        .mustNot(QueryBuilders.termQuery("type", "end")).mustNot(QueryBuilders.termQuery("type", "request"))
        .mustNot(QueryBuilders.termQuery("type", "response")).mustNot(QueryBuilders.termQuery("type", "transfer"))
        .mustNot(QueryBuilders.termQuery("type", "command"));
    //.should(QueryBuilders.rangeQuery("date").gte("2016-02-06T04:12:51.255+0530").lte("2016-02-07T04:12:51.255+0530");

    //queryRangeTime = "now-" + queryRangeTime + "m";

    FilterBuilder fb = FilterBuilders.rangeFilter("date").gte("2016-02-07T04:12:51.255+0530")
        .lte("2016-02-08T04:12:51.255+0530");

    SearchResponse response = transportClient.prepareSearch("versalex-2016-02-07").setTypes("systemlog").setQuery(qb)
        .setPostFilter(fb).setSize(10).execute().actionGet();

    SearchHit hits[] = response.getHits().hits();
    Map<String, Record> tidMapping = new HashMap<>();
    Arrays.stream(hits).forEach(h -> {
      //System.out.println("A hit ...");
      //h.getSource().forEach((k,v) -> System.out.println("Tuple "+k+" , "+v));
      Map source = h.getSource();
      Map atts = (Map) source.get("attributes");
      String ty = (String) atts.get("runtype");
      String threadId = (String) source.get("threadId");
      tidMapping.putIfAbsent(threadId, new Record());
      Record r =  tidMapping.get(threadId);
      r.threadId=threadId;
      if (ty != null)
        r.runtype = ty;
      String prt = (String) atts.get("transport");
      //System.out.println(prt + " , "+threadId + " , "+r.runtype);
      if (prt != null)
        r.transport=prt;
      Integer fz = (Integer) atts.get("fileSize");
      if (fz != null)
        r.filesize=fz;

    });

    tidMapping.forEach((k, v) -> {
      if ("interactive".equals(v.runtype)) {
        System.out.println(v);
      }
    });

    System.out.println("ACTUAL ---->");
    tidMapping.forEach((k, v) -> {
      if ("api".equals(v.runtype)) {
        System.out.println(v);
      }
    });

    //System.out.println(response.toString());
    return response;
  }

  public static void main(String[] args) {
    new ESQuery().searchResultWithAggregation();
  }

  private static class Record {
    private String threadId;
    private String transport;
    private int filesize;
    private String runtype;

    @Override public String toString() {
      return "Record{" +
          "threadId='" + threadId + '\'' +
          ", transport='" + transport + '\'' +
          ", filesize=" + filesize +
          ", runtype='" + runtype + '\'' +
          '}';
    }
  }


}
