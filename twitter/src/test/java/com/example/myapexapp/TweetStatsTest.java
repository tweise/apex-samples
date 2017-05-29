package com.example.myapexapp;

import java.util.List;

import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

import com.datatorrent.lib.appdata.schemas.DataQuerySnapshot;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.schemas.SnapshotSchema;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

public class TweetStatsTest
{
  @Test
  public void testStatsSnapshotServer() throws Exception
  {
    TweetStats tweetStats1 = new TweetStats();
    tweetStats1.timestamp = 1;
    tweetStats1.total = 501;

    TweetStatsSnapshotServer snapshotServer = new TweetStatsSnapshotServer();

    CollectorTestSink<String> resultSink = new CollectorTestSink<>();
    TestUtils.setSink(snapshotServer.queryResult, resultSink);

    List<Object> inputList = Lists.newArrayList();
    inputList.add(tweetStats1);

    snapshotServer.setup(null);

    snapshotServer.beginWindow(0L);
    snapshotServer.input.put(inputList);
    snapshotServer.endWindow();

    Assert.assertEquals(1, resultSink.collectedTuples.size());
    String result = resultSink.collectedTuples.get(0);
    JSONObject data = new JSONObject(result).getJSONArray(DataQuerySnapshot.FIELD_DATA).getJSONObject(0);
    Assert.assertEquals(Long.toString(tweetStats1.timestamp), data.get("timestamp"));
    Assert.assertEquals(Long.toString(tweetStats1.total), data.get("total"));

    resultSink.collectedTuples.clear();

    snapshotServer.beginWindow(1L);
    // new value for existing timestamp
    tweetStats1.total +=1000;

    TweetStats tweetStats2 = new TweetStats();
    tweetStats2.timestamp = 2;
    tweetStats2.total = 502;
    inputList.add(tweetStats2);
    snapshotServer.input.put(inputList);

    Assert.assertEquals(0, resultSink.collectedTuples.size());
    snapshotServer.endWindow();

    Assert.assertEquals(1, resultSink.collectedTuples.size());
    result = resultSink.collectedTuples.get(0);
    data = new JSONObject(result).getJSONArray(DataQuerySnapshot.FIELD_DATA).getJSONObject(0);
    Assert.assertEquals(Long.toString(tweetStats1.timestamp), data.get("timestamp"));
    Assert.assertEquals(Long.toString(tweetStats1.total), data.get("total"));

    // check ordering
    resultSink.collectedTuples.clear();

    snapshotServer.beginWindow(2L);
    TweetStats tweetStats3 = new TweetStats();
    tweetStats3.timestamp = 0;
    tweetStats3.total = 503;
    inputList.add(tweetStats3);
    snapshotServer.input.put(inputList);
    snapshotServer.endWindow();
    result = resultSink.collectedTuples.get(0);
    data = new JSONObject(result).getJSONArray(DataQuerySnapshot.FIELD_DATA).getJSONObject(0);
    Assert.assertEquals(Long.toString(tweetStats3.timestamp), data.get("timestamp"));
    Assert.assertEquals(Long.toString(tweetStats3.total), data.get("total"));
  }

  @Ignore // see APEXMALHAR-2505
  @Test
  public void testFieldOrder() {
    TopNSnapshotServer snapshotServer = new TopNSnapshotServer();
    String JSON = SchemaUtils.jarResourceFileToString(TwitterStatsApp.TOPN_SCHEMA);
    snapshotServer.setSnapshotSchemaJSON(JSON);
    snapshotServer.setup(null);
    SnapshotSchema schema = snapshotServer.getSchema();
    List<String> expectedFieldOrder = Lists.newArrayList("hashtag", "count", "label");
    List<String> actualFieldOrder = Lists.newArrayList(schema.getValuesDescriptor().getFields().getFields());
    Assert.assertEquals(expectedFieldOrder, actualFieldOrder);
  }

}
