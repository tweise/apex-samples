package com.example.myapexapp;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.joda.time.Duration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;
import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.lib.function.FunctionOperator;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Tuple.TimestampedTuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.WindowState;
import org.apache.apex.malhar.lib.window.accumulation.SumLong;
import org.apache.apex.malhar.lib.window.accumulation.TopNByKey;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedKeyedStorage;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedStorage;
import org.apache.apex.malhar.lib.window.impl.KeyedWindowedOperatorImpl;
import org.apache.apex.malhar.lib.window.impl.WindowedOperatorImpl;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;

import com.example.myapexapp.TwitterIngestApp.StringUtils;
import com.google.common.base.Throwables;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.twitter.TwitterSampleInput;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.snapshot.AppDataSnapshotServerMap;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import com.datatorrent.lib.util.KeyValPair;

import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

public class TwitterTopN implements StreamingApplication
{
  private static final Logger LOG = LoggerFactory.getLogger(TwitterTopN.class);
  public static final String SCHEMA = "TwitterTopNSchema.json";
  public static final URI uri = URI.create("ws://localhost:8890/pubsub");
  // TODO: configuration
  public static final String topic = "twitter";
  public static final String queryTopic = "twitterQuery";
  private boolean liveSource = false;

  private static class ExtractHashtags implements Function.FlatMapFunction<String, Tuple.TimestampedTuple<KeyValPair<String, Long>>>
  {
    @Override
    public Iterable<Tuple.TimestampedTuple<KeyValPair<String, Long>>> f(String input)
    {
      try {
        Status status = TwitterObjectFactory.createStatus(input);
        long timestamp = status.getCreatedAt().getTime();
        List<Tuple.TimestampedTuple<KeyValPair<String, Long>>> result = new LinkedList<>();
        HashtagEntity[] hashtags = status.getHashtagEntities();
        for (HashtagEntity hashtag : hashtags) {
          Tuple.TimestampedTuple<KeyValPair<String, Long>> entry = new Tuple.TimestampedTuple<>(timestamp, new KeyValPair<>(hashtag.getText(), 1L));
          result.add(entry);
        }
        return result;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class ExtractHashtagsFromStatus implements Function.FlatMapFunction<Status, Tuple.TimestampedTuple<KeyValPair<String, Long>>>
  {
    @Override
    public Iterable<Tuple.TimestampedTuple<KeyValPair<String, Long>>> f(Status status)
    {
      List<Tuple.TimestampedTuple<KeyValPair<String, Long>>> result = new LinkedList<>();
      long timestamp = status.getCreatedAt().getTime();

      HashtagEntity[] entities = status.getHashtagEntities();
      if (entities != null) {
        for (HashtagEntity he : entities) {
          if (he != null) {
            Tuple.TimestampedTuple<KeyValPair<String, Long>> entry = new Tuple.TimestampedTuple<>(
                timestamp, new KeyValPair<>(he.getText(), 1L));
            result.add(entry);
          }
        }
      }
      return result;
    }
  }

  /*
  private static class TupleToMap implements Function.MapFunction<Tuple.WindowedTuple<List<KeyValPair<String, Long>>>, List<Map<String, Object>>>
  {
    @Override
    public List<Map<String, Object>> f(Tuple.WindowedTuple<List<KeyValPair<String, Long>>> input)
    {
      List<Map<String, Object>> result = new ArrayList<>();
      Long timestamp = input.getTimestamp();
      for (KeyValPair<String, Long> kv : input.getValue()) {
        Map<String, Object> row = new HashMap<>();
        row.put("hashtag", kv.getKey());
        row.put("count", kv.getValue());
        row.put("timestamp", timestamp);
        result.add(row);
      }
      return result;
    }
  }
  */

  private static class TupleToMap implements Function.MapFunction<Tuple.WindowedTuple<List<KeyValPair<String, Long>>>, String>
  {
    @Override
    public String f(Tuple.WindowedTuple<List<KeyValPair<String, Long>>> input)
    {
      Long timestamp = input.getTimestamp();
      try {
        JSONObject result = new JSONObject();
        result.put("id", "topN");
        result.put("event_time_window_timestamp", timestamp.longValue());
        JSONArray table = new JSONArray();
        int rowNum = 0;
        for (KeyValPair<String, Long> kv : input.getValue()) {
          JSONObject row = new JSONObject();
          row.put("hashtag", kv.getKey());
          row.put("count", kv.getValue());
          row.put("rowId", rowNum++);
          table.put(row);
        }
        result.put("result", table);
        return result.toString();
      } catch (JSONException e) {
        throw Throwables.propagate(e);
      }
/*
        int count = 0;
      String result = "{\"id\":\"topN\",\"event_time_window_timestamp\":\"" + timestamp.toString() + "\",\"result\":[";
      for (KeyValPair<String, Long> kv : input.getValue()) {
        result += "{\"hashtag\":\"" + kv.getKey() + "\",\"count\":\"" + kv.getValue().toString() + "\"}";
        if (++count < input.getValue().size()) {
          result += ",";
        }
      }
      result += "]}";
      return result;
*/
    }
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    final OutputPort<TimestampedTuple<KeyValPair<String, Long>>> hashtagsPort;
    if (liveSource) {
      TwitterSampleInput sampleInput = dag.addOperator("twitterSampleInput", new TwitterSampleInput());
      FunctionOperator.FlatMapFunctionOperator<Status, Tuple.TimestampedTuple<KeyValPair<String, Long>>> extractSampleHashtags =
          new FunctionOperator.FlatMapFunctionOperator<>(new ExtractHashtagsFromStatus());
      dag.addOperator("extractHashtags", extractSampleHashtags);
      dag.addStream("rawTweets", sampleInput.status, extractSampleHashtags.input);
      hashtagsPort = extractSampleHashtags.output;
    } else {
      LineByLineFileInputOperator fileInput = new LineByLineFileInputOperator();
      fileInput.setDirectory("./src/test/resources/sampleTweets.txt");
      FunctionOperator.FlatMapFunctionOperator<String, Tuple.TimestampedTuple<KeyValPair<String, Long>>> extractHashtagsOp =
          new FunctionOperator.FlatMapFunctionOperator<>(new ExtractHashtags());
      dag.addOperator("fileInput", fileInput);
      dag.addOperator("extractHashtags", extractHashtagsOp);
      dag.addStream("rawTweets", fileInput.output, extractHashtagsOp.input);
      hashtagsPort = extractHashtagsOp.output;
    }

    KeyedWindowedOperatorImpl<String, Long, MutableLong, Long> countBykeyOp = new KeyedWindowedOperatorImpl<>();
    countBykeyOp.setAccumulation(new SumLong());
    countBykeyOp.setDataStorage(new InMemoryWindowedKeyedStorage<String, MutableLong>());
    countBykeyOp.setWindowOption(new WindowOption.TimeWindows(Duration.standardMinutes(5)));
    countBykeyOp.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    countBykeyOp.setTriggerOption(TriggerOption.AtWatermark().
        withEarlyFiringsAtEvery(Duration.standardSeconds(2)).accumulatingFiredPanes());

    TopNByKey<String, Long> topNByKey = new TopNByKey<>();
    topNByKey.setN(10);
    WindowedOperatorImpl<KeyValPair<String, Long>, Map<String, Long>, List<KeyValPair<String, Long>>> topN = new WindowedOperatorImpl<>();
    topN.setAccumulation(topNByKey);
    topN.setDataStorage(new InMemoryWindowedStorage<Map<String, Long>>());
    topN.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    topN.setTriggerOption(TriggerOption.AtWatermark()
        .withEarlyFiringsAtEvery(Duration.standardSeconds(2))
        .accumulatingFiredPanes());

    //FunctionOperator.MapFunctionOperator<Tuple.WindowedTuple<List<KeyValPair<String, Long>>>, List<Map<String, Object>>> mapOp =
    //        new FunctionOperator.MapFunctionOperator<>(new TupleToMap());

    FunctionOperator.MapFunctionOperator<Tuple.WindowedTuple<List<KeyValPair<String, Long>>>, String> mapOp =
        new FunctionOperator.MapFunctionOperator<>(new TupleToMap());

    AppDataSnapshotServerMap snapshotServerMap = new AppDataSnapshotServerMap();
    String JSON = SchemaUtils.jarResourceFileToString(SCHEMA);
    snapshotServerMap.setSnapshotSchemaJSON(JSON);

    PubSubWebSocketAppDataQuery wsQuery = new PubSubWebSocketAppDataQuery();
    wsQuery.enableEmbeddedMode();
    wsQuery.setUri(uri);
    wsQuery.setTopic(queryTopic);
    snapshotServerMap.setEmbeddableQueryInfoProvider(wsQuery);

    PubSubWebSocketAppDataResult wsResult = new PubSubWebSocketAppDataResult();
    wsResult.setUri(uri);
    wsResult.setTopic(topic);

    ConsoleOutputOperator console = new ConsoleOutputOperator();

    dag.addOperator("countByKey", countBykeyOp);
    dag.addOperator("topN", topN);
    dag.addOperator("TupleToMap", mapOp);
    //dag.addOperator("snapshotServer", snapshotServerMap);
    dag.addOperator("QueryResult", wsResult);
    //dag.addOperator("con", console);

    dag.addStream("hashtags", hashtagsPort, countBykeyOp.input);
    dag.addStream("countingResult", countBykeyOp.output, topN.input);
    dag.addStream("topNResult", topN.output, mapOp.input);
    dag.addStream("resultStrings", mapOp.output, wsResult.input/*, console.input*/);
   // dag.addStream("finalResult", snapshotServerMap.queryResult, wsResult.input, console.input);

  }

  @Test
  public void twitterTopNTest() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    TwitterTopN app = new TwitterTopN();
    app.liveSource = false;
    lma.prepareDAG(app, conf);
    LocalMode.Controller lc = lma.getController();
    lc.run(Duration.standardMinutes(5).getMillis());
  }
}
