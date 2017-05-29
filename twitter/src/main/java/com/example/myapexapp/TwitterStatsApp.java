package com.example.myapexapp;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.joda.time.Duration;

import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;
import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.lib.function.FunctionOperator;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Tuple.TimestampedTuple;
import org.apache.apex.malhar.lib.window.Tuple.WindowedTuple;
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

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import com.datatorrent.lib.util.KeyValPair;

import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

public class TwitterStatsApp implements StreamingApplication
{
  /**
   * Property key to indicate if to use the Twitter sample stream
   * file input with recorded data as source.
   */
  public static String IS_TWITTER_SAMPLE_INPUT = "twitterDemo.isTwitterSampleInput";

  /**
   * Property key to indicate if to output results to websocket.
   * If false, output will be directed to console.
   */
  public static String IS_WEBSOCKET_OUTPUT = "twitterDemo.isWebsocketOutput";

  public static String WEBSOCKET_URI = "twitterDemo.websocketUri";

  public static final String TOPN_SCHEMA = "TwitterTopNSchema.json";

  public static final String topic = "twitter";
  public static final String queryTopic = "twitterQuery";


  static class ExtractHashtags implements Function.FlatMapFunction<Status, Tuple.TimestampedTuple<KeyValPair<String, Long>>>
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

  /**
   * Unwraps {@link WindowedTuple} and converts {@KeyValPair} to map for generic snapshot server.
   */
  static class KeyValPairToMap implements Function.MapFunction<Tuple.WindowedTuple<List<KeyValPair<String, Long>>>, List<Map<String, Object>>>
  {
    @Override
    public List<Map<String, Object>> f(Tuple.WindowedTuple<List<KeyValPair<String, Long>>> input)
    {
      List<Map<String, Object>> result = new ArrayList<>();
      int rowNum = 0;
      for (KeyValPair<String, Long> kv : input.getValue()) {
        Map<String, Object> row = new HashMap<>();
        row.put("hashtag", kv.getKey());
        row.put("count", kv.getValue());
        row.put("label", Integer.toString(rowNum++));
        result.add(row);
      }
      return result;
    }
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    final OutputPort<Status> statusPort;
    boolean isTwitterInput = conf.getBoolean(IS_TWITTER_SAMPLE_INPUT, false);
    boolean isWebsocketOutput = conf.getBoolean(IS_WEBSOCKET_OUTPUT, false);
    URI uri = URI.create(conf.get(WEBSOCKET_URI,"ws://localhost:8890/pubsub"));

    if (isTwitterInput) {
      TwitterSampleInput sampleInput = dag.addOperator("twitterSampleInput", new TwitterSampleInput());
      statusPort = sampleInput.status;
    } else {
      LineByLineFileInputOperator fileInput = new LineByLineFileInputOperator();
      fileInput.setDirectory("./src/test/resources/sampleTweets.txt");
      FunctionOperator.MapFunctionOperator<String, Status> jsonToStatus =
          new FunctionOperator.MapFunctionOperator<>(
              new Function.MapFunction<String, Status>()
              {
                @Override
                public Status f(String input)
                {
                  try {
                      return TwitterObjectFactory.createStatus(input);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                }
              });
      dag.addOperator("fileInput", fileInput);
      dag.addOperator("jsonToStatus", jsonToStatus);
      dag.addStream("rawJson", fileInput.output, jsonToStatus.input).setLocality(Locality.THREAD_LOCAL);
      statusPort = jsonToStatus.output;
    }

    FunctionOperator.FlatMapFunctionOperator<Status, Tuple.TimestampedTuple<KeyValPair<String, Long>>> extractHashtags =
        new FunctionOperator.FlatMapFunctionOperator<>(new ExtractHashtags());

    /**
     * Assign the timestamp that will determine the window for the status.
     * The timestamp can also be extracted by the window operator, but since
     * a type conversion to {@link Tuple} is required, we can also assign it here.
     */
    FunctionOperator.MapFunctionOperator<Status, TimestampedTuple<Status>> assignTimestamp =
        new FunctionOperator.MapFunctionOperator<>(
            new Function.MapFunction<Status, TimestampedTuple<Status>>()
            {
              @Override
              public TimestampedTuple<Status> f(Status input)
              {
                long timestamp = input.getCreatedAt().getTime();
                return new TimestampedTuple<>(timestamp, input);
              }
            }
            );

    /**
     * Compute the metrics for the window.
     */
    WindowedOperatorImpl<Status, TweetStats, TweetStats> windowTweetStats = new WindowedOperatorImpl<>();
    windowTweetStats.setAccumulation(new TweetStatsAccumulation());
    windowTweetStats.setDataStorage(new InMemoryWindowedStorage<TweetStats>());
    windowTweetStats.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    windowTweetStats.setWindowOption(new WindowOption.SlidingTimeWindows(Duration.standardMinutes(1), Duration.standardSeconds(30)));
    windowTweetStats.setTriggerOption(TriggerOption.AtWatermark()
        .withEarlyFiringsAtEvery(Duration.standardSeconds(2))
        .accumulatingFiredPanes());

    /**
     * Assign the window timestamp to the result and convert the window tuple
     * to the list needed by the snapshot operator.
     * Note that the previous operator cannot emit a list and
     * the accumulation also cannot assign the result pane timestamp.
     */
    FunctionOperator.MapFunctionOperator<WindowedTuple<TweetStats>, List<Object>> toTweetStatsList =
        new FunctionOperator.MapFunctionOperator<>(
            new Function.MapFunction<WindowedTuple<TweetStats>, List<Object>>()
            {
              @Override
              public List<Object> f(WindowedTuple<TweetStats> input)
              {
                input.getValue().timestamp = input.getTimestamp();
                return Collections.<Object>singletonList(input.getValue());
              }
            }
            );

    TweetStatsSnapshotServer tweetStatsSnapshot = new TweetStatsSnapshotServer();

    dag.addOperator("extractHashtags", extractHashtags);
    dag.addOperator("assignTimestamp", assignTimestamp);
    dag.addOperator("windowTweetStats", windowTweetStats);
    dag.addOperator("toTweetStatsList", toTweetStatsList);
    dag.addOperator("tweetStatsSnapshot", tweetStatsSnapshot);

    dag.addStream("rawTweets", statusPort, extractHashtags.input, assignTimestamp.input).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("timestampedTweets", assignTimestamp.output, windowTweetStats.input);
    dag.addStream("windowedStats", windowTweetStats.output, toTweetStatsList.input);
    dag.addStream("tweetStatsList", toTweetStatsList.output, tweetStatsSnapshot.input);

    DefaultInputPort<String> tweetStatsResultPort;
    if (isWebsocketOutput) {
      PubSubWebSocketAppDataResult wsResult = dag.addOperator("tweetStatsWebsocket",
          new PubSubWebSocketAppDataResult());
      wsResult.setUri(uri);
      wsResult.setTopic(topic);
      tweetStatsResultPort = wsResult.input;
    } else {
      ConsoleOutputOperator tweetStatsConsole = dag.addOperator("tweetStatsConsole",
          new ConsoleOutputOperator());
      tweetStatsResultPort = (DefaultInputPort)tweetStatsConsole.input;
    }
    dag.addStream("tweetStatsResult", tweetStatsSnapshot.queryResult, tweetStatsResultPort);

    KeyedWindowedOperatorImpl<String, Long, MutableLong, Long> countByKey =
        new KeyedWindowedOperatorImpl<>();
    countByKey.setAccumulation(new SumLong());
    countByKey.setDataStorage(new InMemoryWindowedKeyedStorage<String, MutableLong>());
    countByKey.setWindowOption(new WindowOption.TimeWindows(Duration.standardMinutes(5)));
    countByKey.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    countByKey.setTriggerOption(TriggerOption.AtWatermark().
        withEarlyFiringsAtEvery(Duration.standardSeconds(5)).accumulatingFiredPanes());

    TopNByKey<String, Long> topNByKey = new TopNByKey<>();
    topNByKey.setN(10);
    WindowedOperatorImpl<KeyValPair<String, Long>, Map<String, Long>, List<KeyValPair<String, Long>>> topN = new WindowedOperatorImpl<>();
    topN.setAccumulation(topNByKey);
    topN.setDataStorage(new InMemoryWindowedStorage<Map<String, Long>>());
    topN.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    topN.setTriggerOption(TriggerOption.AtWatermark()
        .withEarlyFiringsAtEvery(Duration.standardSeconds(2))
        .accumulatingFiredPanes());

    FunctionOperator.MapFunctionOperator<Tuple.WindowedTuple<List<KeyValPair<String, Long>>>, List<Map<String, Object>>> kvToMap =
        new FunctionOperator.MapFunctionOperator<>(new KeyValPairToMap());

    TopNSnapshotServer topTagsSnapshot = new TopNSnapshotServer();
    String JSON = SchemaUtils.jarResourceFileToString(TOPN_SCHEMA);
    topTagsSnapshot.setSnapshotSchemaJSON(JSON);

    //PubSubWebSocketAppDataQuery wsQuery = new PubSubWebSocketAppDataQuery();
    //wsQuery.enableEmbeddedMode();
    //wsQuery.setUri(uri);
    //wsQuery.setTopic(queryTopic);
    //topTagsSnapshot.setEmbeddableQueryInfoProvider(wsQuery);

    DefaultInputPort<String> topNResultPort;
    if (isWebsocketOutput) {
      PubSubWebSocketAppDataResult wsResult = dag.addOperator("topNResult",
          new PubSubWebSocketAppDataResult());
      wsResult.setUri(uri);
      wsResult.setTopic(topic);
      topNResultPort = wsResult.input;
    } else {
      ConsoleOutputOperator console = dag.addOperator("topNConsole", new ConsoleOutputOperator());
      topNResultPort = (DefaultInputPort)console.input;
    }

    dag.addOperator("countByKey", countByKey);
    dag.addOperator("topN", topN);
    dag.addOperator("kvToMap", kvToMap);
    dag.addOperator("topTagsSnapshot", topTagsSnapshot);

    dag.addStream("hashtags", extractHashtags.output, countByKey.input);
    dag.addStream("uniqueTags", countByKey.output, topN.input);
    dag.addStream("topTags", topN.output, kvToMap.input);
    dag.addStream("topTagsMap", kvToMap.output, topTagsSnapshot.input);
    dag.addStream("topTagsResult", topTagsSnapshot.queryResult, topNResultPort);

  }

}
