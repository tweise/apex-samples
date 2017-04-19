/**
 * Put your copyright and license info here.
 */
package com.example.myapexapp;

import java.util.Arrays;

import org.joda.time.Duration;

import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.lib.function.Function.MapFunction;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Tuple.TimestampedTuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.stream.api.WindowedStream;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.util.KeyValPair;

import static org.apache.apex.malhar.stream.api.Option.Options.name;

/**
 * This is an example of using the WindowedOperator concepts to do streaming word count.
 */
@ApplicationAnnotation(name="ApplicationWithHighLevelAPI")
public class ApplicationWithHighLevelAPI implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    //populateWithLambda(dag);
    WindowedStream<TimestampedTuple<String>> stream =
    StreamFactory.fromFolder("src/test/resources")
    .flatMap(new Function.FlatMapFunction<String, String>()
      {
        @Override
        public Iterable<String> f(String input)
        {
          return Arrays.asList(input.split("[\\p{Punct}\\s]+"));
        }
      }, name("ExtractWords"))
    .map(new MapFunction<String, TimestampedTuple<String>>()
      {
        @Override
        public TimestampedTuple<String> f(String input)
        {
          return new Tuple.TimestampedTuple<>(System.currentTimeMillis(), input);
        }
      }, name("AddTimestamp"))
    .window(new WindowOption.TimeWindows(Duration.standardMinutes(5)),
          TriggerOption.AtWatermark()
             .accumulatingFiredPanes()
             .withEarlyFiringsAtEvery(Duration.standardSeconds(1)),
          Duration.standardSeconds(15))
    ;
    WindowedStream<Tuple.WindowedTuple<KeyValPair<String, Long>>> wordCounts =
    stream.countByKey(
        new Function.ToKeyValue<Tuple.TimestampedTuple<String>, String, Long>()
        {
          @Override
          public TimestampedTuple<KeyValPair<String, Long>> f(TimestampedTuple<String> input)
          {
            return new TimestampedTuple<>(input.getTimestamp(),new KeyValPair<>(input.getValue(), 1L));
          }
        });
    wordCounts
    .print()
    .populateDag(dag);
  }

  /**
   * As of version 3.7.0 lambda expressions cause serialization errors
   * https://issues.apache.org/jira/browse/APEXMALHAR-2481
   * @param dag
   */
/*
  private void populateWithLambda(DAG dag)
  {
    WindowedStream<TimestampedTuple<String>> stream =
    StreamFactory.fromFolder("/tmp")
    .flatMap(input -> Arrays.asList(input.split("\\s+")))
    .map(input -> new Tuple.TimestampedTuple<>(System.currentTimeMillis(), input))
    .window(new WindowOption.TimeWindows(Duration.standardMinutes(5)),
          TriggerOption.AtWatermark()
             .accumulatingFiredPanes()
             .withEarlyFiringsAtEvery(Duration.standardSeconds(1)),
          Duration.standardSeconds(15))
    ;
    WindowedStream<Tuple.WindowedTuple<KeyValPair<String, Long>>> wordCounts =
      stream.countByKey(input -> new TimestampedTuple<>(input.getTimestamp(),
        new KeyValPair<>(input.getValue(), 1L)));
    wordCounts
    .print()
    .populateDag(dag);
  }
*/
}
