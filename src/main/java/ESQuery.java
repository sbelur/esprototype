import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.stream.Stream;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;

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

    Calendar now = Calendar.getInstance();
    now.set(Calendar.MILLISECOND, 0);
    now.set(Calendar.SECOND, 0);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    String endtime = sdf.format(now.getTime());

    String indexName = "aggregation-input";
    createIndex(indexName);
    createIndex("aggregation-meta");
    createIndex("rtaggregation");

    String startTime = null;
    SearchResponse metaResp = transportClient.prepareSearch("aggregation-meta").setTypes("meta")
        .setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
    System.out.println(metaResp.toString());
    long metahitcount = metaResp.getHits().getTotalHits();
    if (metahitcount == 0) {
      now.set(Calendar.DATE, now.get(Calendar.DATE) - 7);
      now.set(Calendar.MINUTE, 0);
      startTime = sdf.format(now.getTime());
    }
    // todo fill starttime if not null

    QueryBuilder qb = QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("type", "detail"))
        .mustNot(QueryBuilders.termQuery("type", "end")).mustNot(QueryBuilders.termQuery("type", "request"))
        .mustNot(QueryBuilders.termQuery("type", "response")).mustNot(QueryBuilders.termQuery("type", "transfer"))
        .mustNot(QueryBuilders.termQuery("type", "command"));
    //.should(QueryBuilders.rangeQuery("date").gte("2016-02-06T04:12:51.255+0530").lte("2016-02-07T04:12:51.255+0530");

    //queryRangeTime = "now-" + queryRangeTime + "m";
    FilterBuilder fb = FilterBuilders.rangeFilter("date").gte(startTime).lte(endtime);

    SearchResponse response = transportClient.prepareSearch("versalex-2016-02-07").setTypes("systemlog").setQuery(qb)
        .setPostFilter(fb).setSize(10000).execute().actionGet();//todo - paginate

    SearchHit hits[] = response.getHits().hits();
    Map<String, List<Record>> tidMapping = new HashMap<>();
    Arrays.stream(hits).forEach(h -> {
      //System.out.println("A hit ...");
      //h.getSource().forEach((k,v) -> System.out.println("Tuple "+k+" , "+v));
      Map source = h.getSource();
      Map atts = (Map) source.get("attributes");
      String ty = (String) atts.get("runtype");
      String threadId = (String) source.get("threadId");
      String file = (String) atts.get("source");
      String dt = (String) source.get("date");

      tidMapping.putIfAbsent(threadId, new ArrayList<Record>());
      List<Record> list = tidMapping.get(threadId);
      Stream<Record> recordStream = list.stream().filter(r -> r.date.equals(dt));
      Record r = null;
      Optional<Record> first = recordStream.findFirst();
      if (first.isPresent()) {
        r = first.get();
      } else {
        r = new Record();
        list.add(r);
      }
      r.threadId = threadId;
      if (file != null)
        r.source = file;

      if (dt != null)
        r.date = dt;

      if (ty != null)
        r.runtype = ty;
      else {
        Optional<Record> aRec = list.stream().filter(rec -> rec.threadId.equals(threadId))
            .filter(record -> record.runtype != null).findFirst();
        if (aRec.isPresent()) {
          r.runtype = aRec.get().runtype;
        }
      }

      String prt = (String) atts.get("transport");
      //System.out.println(prt + " , "+threadId + " , "+r.runtype);
      if (prt != null)
        r.transport = prt;
      Integer fz = (Integer) atts.get("fileSize");
      if (fz != null)
        r.filesize = fz;

    });

    tidMapping.forEach((thread, recList) -> {
      recList.stream().forEach(rec -> {
        if (null != rec.runtype && !rec.runtype.isEmpty() && !rec.runtype.equals("startup")) {
          try {
            IndexResponse indexResponse = transportClient.prepareIndex(indexName, "rawdata").setSource(
                jsonBuilder().startObject().field("transport", rec.transport).field("filesize", rec.filesize)
                    .field("runtype", rec.runtype).field("date", rec.date).field("source", rec.source).endObject())
                .execute().actionGet();
          } catch (IOException e) {
            //throw new RuntimeException(e);
            e.printStackTrace();
          }
        }
      });
    });

    qb = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("runtype", "api"));

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    SearchResponse sresponse = transportClient.prepareSearch(indexName).setTypes("rawdata").setQuery(qb)
        //.setPostFilter(fb)

            .addAggregation(AggregationBuilders.terms("protocol").field("transport").subAggregation(
                AggregationBuilders.dateHistogram("date").field("date").interval(DateHistogram.Interval.MINUTE)
                    .subAggregation(AggregationBuilders.avg("size_avg").field("filesize"))
                    .subAggregation(AggregationBuilders.min("size_min").field("filesize"))
                    .subAggregation(AggregationBuilders.max("size_max").field("filesize")))).setSize(0).execute()
        .actionGet();

    if (sresponse.getHits().getTotalHits() > 0) {
      System.out.println(sresponse.toString());
    }
   /* Aggregations aggs = ((StringTerms) sresponse.getAggregations().asList().get(0)).getBuckets().get(0).getAggregations();
    List<Aggregation> aggregationList = aggs.asList();
    Aggregation aggregation= aggregationList.get(0);
    List<? extends Histogram.Bucket> buckets = ((Histogram)aggregation).getBuckets();
    Histogram.Bucket aB = buckets.get(0);
    List<Aggregation> l= ((Aggregations)((Histogram.Bucket)((Histogram)((StringTerms) sresponse.getAggregations().asList().get(0)).getBuckets().get(0).getAggregations().asList().get(0)).getBuckets().get(0)).getAggregations()).asList();
    NumericMetricsAggregation.SingleValue avg = (NumericMetricsAggregation.SingleValue)l.get(0);
    avg.value();*/

    sresponse.getAggregations().asList().stream().forEach(agg->{
      System.out.println();
      ((StringTerms)agg).getBuckets().stream().forEach(e->{
        Aggregations aggs  = e.getAggregations();
        List<Aggregation> aggregationList = aggs.asList();
        aggregationList.stream().forEach(aggregation ->  {
              List<? extends Histogram.Bucket> buckets = ((Histogram)aggregation).getBuckets();
              buckets.stream().forEach(aB->{
                List<Aggregation> aggList = aB.getAggregations().asList();
                System.out.println("("+e.getKey()+","+aB.getKey()+")"+"=>");
                aggList.stream().forEach(entry->{
                  NumericMetricsAggregation.SingleValue val = (NumericMetricsAggregation.SingleValue)entry;
                  System.out.println(val.getName() + ", "+val.value());
                });
              });
            }
        );
      });
    });


    //System.out.println(sresponse.toString());
    return response;
  }

  private void createIndex(String indexName) {
    if (!indexExists(indexName)) {
      CreateIndexRequestBuilder cirb = transportClient.admin().indices().prepareCreate(indexName);
      CreateIndexResponse createIndexResponse = cirb.execute().actionGet();
      if (!createIndexResponse.isAcknowledged())
        throw new RuntimeException("Could not create index [" + indexName + "].");
    }
  }

  public static void main(String[] args) {
    new ESQuery().searchResultWithAggregation();
  }

  private static class Record {
    private String threadId;
    private String transport;
    private int filesize;
    private String runtype;
    public String date;
    public String source;

    @Override public String toString() {
      return "Record{" +
          "threadId='" + threadId + '\'' +
          ", transport='" + transport + '\'' +
          ", filesize=" + filesize +
          ", date=" + date +
          ", source=" + source +
          ", runtype='" + runtype + '\'' +
          '}';
    }

  }

  private boolean indexExists(String index) {
    return transportClient.admin().indices().prepareExists(index).execute().actionGet().isExists();
  }

}
