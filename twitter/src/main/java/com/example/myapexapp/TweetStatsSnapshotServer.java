package com.example.myapexapp;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import com.google.common.collect.Maps;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.DataQuerySnapshot;
import com.datatorrent.lib.appdata.schemas.Fields;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.snapshot.AbstractAppDataSnapshotServer;
import com.datatorrent.lib.appdata.snapshot.AppDataSnapshotServerPOJO;

/**
 *  An implementation of {@link AbstractAppDataSnapshotServer} that serves
 *  time series data for a configurable count of most recent upstream window results.
 */
public class TweetStatsSnapshotServer extends AppDataSnapshotServerPOJO
{
  private int maxResultEntries = 10;
  private transient TreeSet<GPOMutable> results = new TreeSet<>(new GPOMutableComparator());

  public TweetStatsSnapshotServer()
  {
    // this can be obtained by inspecting the class...
    Map<String, String> fieldToGetter = Maps.newHashMap();
    fieldToGetter.put("timestamp", "timestamp");
    fieldToGetter.put("total", "total");
    fieldToGetter.put("withURL", "withURL");
    fieldToGetter.put("withHashtag", "withHashtag");
    super.setFieldToGetter(fieldToGetter);

    this.snapshotSchemaJSON = SchemaUtils.jarResourceFileToString("tweetStatsPojoSchema.json");
  }

  public int getMaxResultEntries()
  {
    return maxResultEntries;
  }

  public void setMaxResultEntries(int maxResultEntries)
  {
    this.maxResultEntries = maxResultEntries;
  }

  @Override
  protected void processData(List<Object> rows)
  {
    results.clear();
    for (Object inputEvent : rows) {
      GPOMutable gpoRow = convert(inputEvent);
      results.add(gpoRow);
    }
    // retains first added entry when equal (most recent value)
    results.addAll(currentData);

    currentData.clear();
    GPOMutable gpoRow;
    Iterator<GPOMutable> it = results.iterator();
    for (int i = 0; it.hasNext(); i++) {
      gpoRow = it.next();
      if (results.size() - i <= maxResultEntries) {
        // carry over most recent rows
        currentData.add(gpoRow);
      }
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    // emit results automatically
    Fields fields = new Fields(super.getFieldToGetter().values());
    DataQuerySnapshot query = new DataQuerySnapshot("tweetStats", fields);
    queryProcessor.enqueue(query, null, null);
  }

  public static class GPOMutableComparator implements Comparator<GPOMutable>
  {
    String field = "timestamp";
    @Override
    public int compare(GPOMutable o1, GPOMutable o2)
    {
      Object f1 = o1.getField(field);
      Object f2 = o2.getField(field);
      return ((Comparable)f1).compareTo(f2);
    }
  }

}
