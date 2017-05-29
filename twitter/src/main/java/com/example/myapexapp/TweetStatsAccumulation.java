package com.example.myapexapp;

import org.apache.apex.malhar.lib.window.Accumulation;

import twitter4j.Status;

/**
 * Accumulation for {@link TweetStats} based on Twitter {@link Status} inputs.
 */
public class TweetStatsAccumulation implements Accumulation<Status, TweetStats, TweetStats>
{
  @Override
  public TweetStats defaultAccumulatedValue()
  {
    return new TweetStats();
  }

  @Override
  public TweetStats accumulate(TweetStats accumulatedValue, Status input)
  {
    accumulatedValue.total++;
    if (input.getHashtagEntities().length > 0) {
      accumulatedValue.withHashtag++;
    }
    if (input.getURLEntities().length > 0) {
      accumulatedValue.withURL++;
    }
    return accumulatedValue;
  }

  @Override
  public TweetStats merge(TweetStats accumulatedValue1, TweetStats accumulatedValue2)
  {
    accumulatedValue1.total += accumulatedValue2.total;
    accumulatedValue1.withHashtag += accumulatedValue2.withHashtag;
    accumulatedValue1.withURL += accumulatedValue2.withURL;
    return accumulatedValue1;
  }

  @Override
  public TweetStats getOutput(TweetStats accumulatedValue)
  {
    return accumulatedValue;
  }

  @Override
  public TweetStats getRetraction(TweetStats value)
  {
    throw new UnsupportedOperationException();
  }
}
