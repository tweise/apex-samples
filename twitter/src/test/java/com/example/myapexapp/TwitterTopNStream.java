package com.example.myapexapp;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.joda.time.Duration;
import org.junit.Test;

import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.accumulation.TopN;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import com.datatorrent.lib.util.KeyValPair;

import static org.apache.apex.malhar.stream.api.Option.Options.name;

public class TwitterTopNStream
{
  private static class ExtractHashtags implements Function.FlatMapFunction<String, KeyValPair<String, Long>>
  {
    @Override
    public Iterable<KeyValPair<String, Long>> f(String input)
    {
      List<KeyValPair<String, Long>> result = new LinkedList<>();
      String[] splited = input.split(" ", 2);

      long timestamp = Long.parseLong(splited[0]);
      Matcher m = Pattern.compile("#\\S+").matcher(splited[1]);
      while (m.find()) {
        KeyValPair<String, Long> entry = new KeyValPair<>(m.group().substring(1), timestamp);
        result.add(entry);
      }

      return result;
    }
  }

  public static class TimestampExtractor implements com.google.common.base.Function<KeyValPair<Long, KeyValPair<String, Long>>, Long>
  {
    @Override
    public Long apply(@Nullable KeyValPair<Long, KeyValPair<String, Long>> input)
    {
      return input.getKey();
    }
  }

  public static class Comp implements Comparator<KeyValPair<Long, KeyValPair<String, Long>>>
  {
    @Override
    public int compare(KeyValPair<Long, KeyValPair<String, Long>> o1, KeyValPair<Long, KeyValPair<String, Long>> o2)
    {
      return Long.compare(o1.getValue().getValue(), o2.getValue().getValue());
    }
  }

  @Test
  public void TwitterTopNTest()
  {
    WindowOption windowOption = new WindowOption.TimeWindows(Duration.standardSeconds(5));
    TopN<KeyValPair<Long, KeyValPair<String, Long>>> topN = new TopN<>();
    topN.setN(5);
    topN.setComparator(new Comp());

    ApexStream<KeyValPair<String, Long>> tags = StreamFactory.fromFolder("sampleTweets.txt", name("tweetSampler"))
        .flatMap(new ExtractHashtags());

    tags.window(windowOption, new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(1))
        .countByKey(new Function.ToKeyValue<KeyValPair<String,Long>, String, Long>()
        {
          @Override
          public Tuple<KeyValPair<String, Long>> f(KeyValPair<String, Long> input)
          {
            return new Tuple.TimestampedTuple<>(input.getValue(), new KeyValPair<>(input.getKey(), 1L));
          }
        })
        .map(new Function.MapFunction<Tuple.WindowedTuple<KeyValPair<String,Long>>, KeyValPair<Long, KeyValPair<String, Long>>>()
        {
          @Override
          public KeyValPair<Long, KeyValPair<String, Long>> f(Tuple.WindowedTuple<KeyValPair<String, Long>> input)
          {
            return new KeyValPair<Long, KeyValPair<String, Long>>(input.getTimestamp(), input.getValue());
          }
        })
        .window(windowOption, new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(1))
        .accumulate(topN)
        .with("timestampExtractor", new TimestampExtractor())
        .print(name("console"))
        .runEmbedded(false, 60000, new Callable<Boolean>()
        {
          @Override
          public Boolean call() throws Exception
          {
            return false;
          }
        });

  }
}
