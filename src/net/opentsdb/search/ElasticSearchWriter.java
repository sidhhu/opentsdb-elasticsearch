package net.opentsdb.search;
import httpfailover.FailoverHttpClient;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.stats.StatsCollector;

import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.stumbleupon.async.Deferred;





public class ElasticSearchWriter extends SearchPlugin implements Runnable {
  private String[] HOSTS;
  private TransportClient client;
  private String clustername;
  BulkRequestBuilder bulkRequest;
  LinkedBlockingDeque<TimeseriesEvent> blockingQueue = new LinkedBlockingDeque<>(10000);
  private long lastIndexedTime = System.currentTimeMillis();



  public ElasticSearchWriter() {

  }

  public Client createClient() {
    try {
      Settings settings = Settings.settingsBuilder()
          .put("cluster.name", clustername).build();
      this.client = TransportClient.builder().settings(settings).build();
      for (String host : HOSTS) {
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), 9300));
      }

    } catch (UnknownHostException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    bulkRequest = client.prepareBulk();

    return this.client;
  }
  
  /**
   * Creates a new index in Elastic Search
   * @param index
   */
  private void createIndex(String index) {
    Settings indexSettings;
    try {
      XContentBuilder  builder = XContentFactory.jsonBuilder()
          .startObject()
          .startObject("analysis")
          .startObject("filter")
          .startObject("ngram_filter")
          .field("type", "nGram")
          .field("min_gram", 2) // we need it from be 3 atleast for looking up something like "match": cpu
          .field("max_gram" , 8) // do we really need 8?
          .endObject()
          .endObject()
          .startObject("analyzer")
          .startObject("ngram_filter_analyzer")
          .field("type", "custom")
          .field("tokenizer", "keyword")
          .field("filter", new String[]{"ngram_filter", "lowercase"})
          .endObject()
          .endObject()
          .startObject("analyzer")
          .startObject("lowercase_analyzer_keyword")
          .field("tokenizer", "keyword")
          .field("filter", "lowercase")
          .endObject()
          .endObject()
          .endObject()
          .endObject();
      
      indexSettings = Settings.settingsBuilder()
          .put("number_of_shards", 5)
          .put("number_of_replicas", 1)
        
          .loadFromSource( builder.string()).build();

      JsonParser parser = new JsonParser();
      InputStream  inputReader =  this.getClass().getClassLoader().getResourceAsStream("ts_map.json");
      BufferedReader reader =new BufferedReader(new InputStreamReader(inputReader));

      Object obj_ts = parser.parse(reader);
      JsonObject ts = (JsonObject) obj_ts;

      client.admin().indices().prepareCreate(index).setSettings(indexSettings).addMapping("ts", ts.toString()).execute().actionGet();
      client.admin().cluster().prepareHealth()
      .setWaitForYellowStatus().execute().actionGet();
    } catch (Exception e) {
     //TODO LOG.error("Error creating new index in Elastic Search", e);
    }

  }

  public void queue(TimeseriesEvent event) {

    try {
      blockingQueue.put(event);

    } catch (InterruptedException e) {
      System.out.println("remove later"); //TODO
    }
  }

  public void run() {
    long threadId = Thread.currentThread().getId();
    Thread.currentThread().setName("ElasticSearchWriter-"+threadId);
    while (true) { // catch throwable 
      try {
        processBulk();
        TimeseriesEvent event = blockingQueue.take();
        addDoc(event);

      } catch (InterruptedException e) {
        System.err.println("Error taking the event from the queue"); //TODO
      } catch (NoNodeAvailableException ex) {
        System.err.println("Node not available, creating client again"); //TODO
        client.close();
        try {
          Thread.sleep(60 * 1000);
        } catch (InterruptedException e) {
          System.err.println("Error sleeping thread"); //TODO
        }
        createClient();

      } catch (Throwable th) {
        //TODO
      }
    }
  }

  public void processBulk() {
    BulkResponse bulkResponse;
    try {
      if (bulkRequest.numberOfActions() >= 2000 //TODO 
          || (System.currentTimeMillis() - lastIndexedTime) >= 300000) {
        lastIndexedTime = System.currentTimeMillis();
        bulkResponse = bulkRequest.execute().actionGet();
        //TODO          LOG.info("Time taken for the bulk request to process " +bulkResponse.getTookInMillis() + " and queue size " +blockingQueueNAMT.size() + 
        // TODO              " bulk request size is "+bulkRequest.numberOfActions() );


        for (BulkItemResponse item : bulkResponse.getItems()) {

          handleResponse(item);
        }
        bulkRequest = client.prepareBulk();
        //TODO       LOG.info("Elastic search removed " +count+ " and inflightmap is " +inFlightMap.size() );

      }
    } catch (ElasticsearchException e) {
      //TODO      LOG.error("Exception in processing bulk request", e);
    }  

  }

  private void handleResponse(BulkItemResponse item) {
    //TODO, No idea what to do here. :(

  }

  public void addDoc(TimeseriesEvent event) {

    HashMap<String, Object> namtMap = new HashMap<>();   

    namtMap.put("metric", event.getMetric());

    namtMap.put("tags", event.getTagsNested());

    namtMap.put("tsuid", event.getTsuid().getTSUID());

    namtMap.put("uid_type", event.getUidsNested());


    IndexRequest namtRequest = new IndexRequest("tsdb", "ts").source(namtMap);
    bulkRequest.add(namtRequest);

  }

  @Override
  public void initialize(final TSDB tsdb) {
    config = new ESPluginConfig(tsdb.getConfig());
    setConfiguration();

  }

  @Override
  public Deferred<Object> shutdown() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String version() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void collectStats(StatsCollector collector) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Deferred<Object> indexTSMeta(TSMeta meta) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Object> deleteTSMeta(String tsuid) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Object> indexUIDMeta(UIDMeta meta) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Object> deleteUIDMeta(UIDMeta meta) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Object> indexAnnotation(Annotation note) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Object> deleteAnnotation(Annotation note) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<SearchQuery> executeQuery(SearchQuery query) {
    // TODO Auto-generated method stub
    return null;
  }


}

