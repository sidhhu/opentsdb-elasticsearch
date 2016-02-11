package net.opentsdb.search;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;

public class TimeseriesEvent {
  private String metric;
  private TSMeta tsuid;
  protected Map<String, String> tags = new HashMap<String, String>();
  protected Map<String, UIDMeta> uids = new HashMap<String, UIDMeta>();
  
  
  public String getMetric() {
    return metric;
  }
  public void setMetric(String metric) {
    this.metric = metric;
  }
  public TSMeta getTsuid() {
    return tsuid;
  }
  public void setTsuid(TSMeta tsuid) {
    this.tsuid = tsuid;
  }
 
   
  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }
  
  public void setUid(Map<String, UIDMeta> uids) {
    this.uids = uids;
  }
  
  public List<Map<String, String>> getTagsNested() {
    ArrayList<Map<String, String>> entryList = new ArrayList<>(tags.entrySet().size());
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      HashMap<String, String> keyValuePair = new HashMap<>(2);
      keyValuePair.put("key", entry.getKey());
      keyValuePair.put("value", entry.getValue());
      entryList.add(keyValuePair);
    }
    return entryList;
  }
  
  public List<Map<String, Object>> getUidsNested() {
    ArrayList<Map<String, Object>> entryList = new ArrayList<>(uids.entrySet().size());
    for (Entry<String, UIDMeta> entry : uids.entrySet()) {
      HashMap<String, Object> keyValuePair = new HashMap<>(2);
      keyValuePair.put("type", entry.getKey());
      keyValuePair.put("value", entry.getValue());
      entryList.add(keyValuePair);
    }
    return entryList;
  }
  
  
}
