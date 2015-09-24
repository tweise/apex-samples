/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example.deduper;

import javax.validation.constraints.NotNull;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

import com.datatorrent.lib.bucket.AbstractBucket;
import com.datatorrent.lib.bucket.AbstractTimeBasedBucketManager;
import com.datatorrent.lib.dedup.AbstractDeduper;
import com.datatorrent.lib.util.PojoUtils;

/**
 * @category Filter
 */
public class DeduperImpl extends AbstractDeduper<Object, Object> implements Operator.ActivationListener<Context.OperatorContext>
{
  @NotNull
  private String keyExpression;
  @NotNull
  private String timeExpression;

  private transient PojoUtils.Getter<Object, Object> keyGetter;
  private transient PojoUtils.GetterLong<Object> timeGetter;

  /**
   * The input port on which events are received.
   */
  @InputPortFieldAnnotation(optional = true, schemaRequired = true)
  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {

    @Override
    public void setup(Context.PortContext context)
    {
      Class<?> tupleClass = context.getValue(Context.PortContext.TUPLE_CLASS);
      keyGetter = PojoUtils.createGetter(tupleClass, keyExpression, Object.class);
      timeGetter = PojoUtils.createGetterLong(tupleClass, timeExpression);
    }

    @Override
    public final void process(Object tuple)
    {
      DeduperImpl.super.input.process(tuple);
    }
  };

  public DeduperImpl()
  {
    this.bucketManager = new BucketManagerImpl();
  }

  @Override
  public void activate(Context.OperatorContext context)
  {
    ((BucketManagerImpl)bucketManager).timeGetter = timeGetter;
    ((BucketManagerImpl)bucketManager).keyGetter = keyGetter;
  }

  @Override
  public void deactivate()
  {

  }

  @Override
  protected Object getEventKey(Object o)
  {
    return keyGetter.get(o);
  }

  @Override
  protected Object convert(Object o)
  {
    return o;
  }

  /**
   * @return key expression
   */
  public String getKeyExpression()
  {
    return keyExpression;
  }

  /**
   * Sets the expression which is used to get the key from tuples.
   *
   * @param keyExpression expression for key.
   * @useSchema $ input.fields[].name
   */
  public void setKeyExpression(String keyExpression)
  {
    this.keyExpression = keyExpression;
  }

  /**
   * @return time expression
   */
  public String getTimeExpression()
  {
    return timeExpression;
  }

  /**
   * Sets the expression which is used to get the time from tuples.
   *
   * @param timeExpression expression for time.
   * @userSchema $ input.fields[].name
   */
  public void setTimeExpression(String timeExpression)
  {
    this.timeExpression = timeExpression;
  }

  public static class BucketManagerImpl extends AbstractTimeBasedBucketManager<Object>
  {

    private transient PojoUtils.GetterLong<Object> timeGetter;
    private transient PojoUtils.Getter<Object, Object> keyGetter;

    @Override
    protected long getTime(Object o)
    {
      return timeGetter.get(o);
    }

    @Override
    protected AbstractBucket<Object> createBucket(long l)
    {
      return new Bucket(l, this);
    }

    PojoUtils.Getter<Object, Object> getKeyGetter()
    {
      return keyGetter;
    }
  }

  public static class Bucket extends AbstractBucket<Object>
  {
    private final transient BucketManagerImpl manager;

    protected Bucket()
    {
      super();
      manager = null;
    }

    protected Bucket(long a, BucketManagerImpl manager)
    {
      super(a);
      this.manager = Preconditions.checkNotNull(manager);
    }

    @Override
    protected Object getEventKey(Object o)
    {
      return manager.getKeyGetter().get(o);
    }
  }

}
