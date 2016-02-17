import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by sbelur on 07/02/16.
 */
public class ESQuery {

  private TransportClient transportClient;

  private static Log log = LogFactory.getLog(ESQuery.class);

  Optional<String> lastAggTime = Optional.<String>empty();

  public ESQuery() {
    Settings settings = ImmutableSettings.settingsBuilder().put("http.port", 9200)
        .put("cluster.name", "cleo.elasticsearch").build();

    transportClient = new TransportClient(settings);
    transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

  }

  public void searchResultWithAggregation() {

    try {
      Calendar now = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
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
        now.set(Calendar.MINUTE, now.get(Calendar.MINUTE) - 5);
        now.set(Calendar.SECOND, 0);
        now.set(Calendar.MILLISECOND, 0);
        startTime = sdf.format(now.getTime());
      } else {
        Optional<SearchHit> lastAggMeta = Arrays.stream(metaResp.getHits().getHits()).findFirst();
        if (lastAggMeta.isPresent()) {
          startTime = (String) lastAggMeta.get().getSource().get("lastaggregated");
        }
      }
      lastAggTime = Optional.of(startTime);
      QueryBuilder qb = QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("type", "detail"))
          .mustNot(QueryBuilders.termQuery("type", "end")).mustNot(QueryBuilders.termQuery("type", "request"))
          .mustNot(QueryBuilders.termQuery("type", "response")).mustNot(QueryBuilders.termQuery("type", "transfer"))
          .mustNot(QueryBuilders.termQuery("type", "command"));
      log.info("********* startTime " + startTime + " end time " + endtime);
      FilterBuilder fb = FilterBuilders.rangeFilter("date").gte(startTime).lt(endtime);

      int dat = Calendar.getInstance().get(Calendar.DATE);
      int month = Calendar.getInstance().get(Calendar.MONTH);
      String sm = "" + (month + 1);
      if(sm.length() < 2)sm="0"+sm;
      SearchResponse response = transportClient.prepareSearch("versalex-2016-" +sm + "-" + dat)
          .setTypes("systemlog").setQuery(qb).setPostFilter(fb).setSize(10000).execute().actionGet();//todo - paginate

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

      if (hits.length > 0) {
        FilterBuilder fb1 = FilterBuilders.rangeFilter("date").gte(startTime).lt(endtime);
        FilterBuilder fb2 = FilterBuilders.boolFilter().must(FilterBuilders.termFilter("runtype", "api"));
        FilterBuilder andf = FilterBuilders.andFilter(fb1, fb2);
        SearchResponse sresponse = transportClient.prepareSearch(indexName).setTypes("rawdata")
            .addAggregation(new FilterAggregationBuilder("filtered").filter(andf)

                .subAggregation(AggregationBuilders.terms("protocol").field("transport").subAggregation(
                    AggregationBuilders.dateHistogram("date").field("date").interval(DateHistogram.Interval.MINUTE)
                        .subAggregation(AggregationBuilders.avg("size_avg").field("filesize"))
                        .subAggregation(AggregationBuilders.min("size_min").field("filesize"))
                        .subAggregation(AggregationBuilders.count("reccount").field("filesize"))
                        .subAggregation(AggregationBuilders.max("size_max").field("filesize"))))).setSize(0).execute()
            .actionGet();

        sresponse.getAggregations().asList().stream().forEach(aggf -> {
          ((Filter) aggf).getAggregations().asList().stream().forEach(agg -> {
            ((Terms) agg).getBuckets().stream().forEach(e -> {
              Aggregations aggs = e.getAggregations();
              List<Aggregation> aggregationList = aggs.asList();
              aggregationList.stream().forEach(aggregation -> {
                List<? extends Histogram.Bucket> buckets = ((Histogram) aggregation).getBuckets();
                buckets.stream().forEach(aB -> {
                  List<Aggregation> aggList = aB.getAggregations().asList();
                  Map<String, Object> metrics = new HashMap<String, Object>();
                  aggList.stream().forEach(entry -> {
                    NumericMetricsAggregation.SingleValue val = (NumericMetricsAggregation.SingleValue) entry;
                    metrics.put(val.getName(), val.value());
                  });
                  metrics.put("transport", e.getKey());
                  metrics.put("date", aB.getKey());
                  IndexResponse indexResponse = transportClient.prepareIndex("rtaggregation", "agg").setSource(metrics)
                      .execute().actionGet();
                });
              });
            });
          });
        });
      }

      IndexRequest indexRequest = null;

      indexRequest = new IndexRequest("aggregation-meta", "meta", "1")
          .source(jsonBuilder().startObject().field("lastaggregated", endtime).endObject());

      UpdateRequest updateRequest = null;

      updateRequest = new UpdateRequest("aggregation-meta", "meta", "1")
          .doc(jsonBuilder().startObject().field("lastaggregated", endtime).endObject()).upsert(indexRequest);
      transportClient.update(updateRequest).get();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }

    //System.out.println(sresponse.toString());
    //return response;
  }

  private void createIndex(String indexName) {
    if (!indexExists(indexName)) {
      CreateIndexRequestBuilder cirb = transportClient.admin().indices().prepareCreate(indexName);
      CreateIndexResponse createIndexResponse = cirb.execute().actionGet();
      if (!createIndexResponse.isAcknowledged())
        throw new RuntimeException("Could not create index [" + indexName + "].");
    }
  }

  public SumHolder getGlobalStats() {
    SumHolder holder = new SumHolder();
    try {

      Calendar now = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
   /* now.set(Calendar.MINUTE, 9);
    now.set(Calendar.MONTH, 1);
    now.set(Calendar.DATE, 7);
    now.set(Calendar.HOUR_OF_DAY, 5);*/
      int min = now.get(Calendar.MINUTE);
      int rem = min % 5;
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
      String startTime = null;
      String endtime = null;
      Calendar temp = now;
      temp.set(Calendar.MILLISECOND, 0);
      temp.set(Calendar.SECOND, 0);
      if (rem == 0) {
        startTime = sdf.format(temp.getTime());
        temp.set(Calendar.MINUTE, min + 5);
        endtime = sdf.format(temp.getTime());
      } else {
        temp.set(Calendar.MINUTE, min - rem);
        startTime = sdf.format(temp.getTime());
        temp.set(Calendar.MINUTE, min + (5 - rem));
        endtime = sdf.format(temp.getTime());
      }
      log.info("Checking for " + startTime + "," + endtime);
      //QueryBuilder all = QueryBuilders.boolQuery().
      FilterBuilder fb = FilterBuilders.rangeFilter("date").gte(startTime).lt(endtime);
      SearchResponse response = transportClient.prepareSearch("rtaggregation").setTypes("agg").addAggregation(
          new FilterAggregationBuilder("filtered").filter(fb).subAggregation(
              AggregationBuilders.terms("protocol").field("transport")
                  .subAggregation(AggregationBuilders.sum("total").field("reccount")))).setSize(0).execute()
          .actionGet();
      log.info("total in period " + response.toString());
      response.getAggregations().asList().stream().forEach(agg -> {
        Aggregations aggregations = ((Filter) agg).getAggregations();
        aggregations.asList().stream().forEach(a -> {
          ((Terms) a).getBuckets().stream().forEach(b -> {
            List<Aggregation> ba = b.getAggregations().asList();
            ba.stream().forEach(s -> {
              double value = ((NumericMetricsAggregation.SingleValue) s).value();
              holder.setTotal((int) value);
            });
          });
        });
      });
      log.info("holder value" + holder.getTotal());
    } catch (Exception e) {
      if (!(e instanceof org.elasticsearch.indices.IndexMissingException)) {
        e.printStackTrace();//org.elasticsearch.indices.IndexMissingException
      }
    }
    return holder;
  }

  /*public static void main(String[] args) throws Exception {
    ESQuery esQuery = new ESQuery();
    SumHolder holder = new SumHolder();
    ScheduledFuture f = esQuery.getGlobalStats(holder);
    //esQuery.searchResultWithAggregation();
    f.get(10,TimeUnit.SECONDS);
    System.out.println("Found hits "+holder.getTotal());
  }*/

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

  public static class SumHolder {
    private int total;

    public void setTotal(int total) {
      this.total = total;
    }

    public int getTotal() {
      return total;
    }
  }

}
