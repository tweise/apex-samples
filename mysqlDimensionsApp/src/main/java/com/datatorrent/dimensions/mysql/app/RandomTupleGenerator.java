/*
 * Copyright (c) 2015 DataTorrent
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.dimensions.mysql.app;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Random;

public class RandomTupleGenerator implements InputOperator
{
  public final transient DefaultOutputPort<DataTuple> output = new DefaultOutputPort<DataTuple>();

  private int numTuplesPerWindow = 100;
  private int windowCount = 0;

  public static final List<String> NODE_TYPE = Lists.newArrayList("TYPE_A", "TYPE_B", "TYPE_C");
  public static final List<String> NODE_NAME = Lists.newArrayList("NAME_1", "NAME_2", "NAME_3");

  private transient Random rand = new Random();

  public RandomTupleGenerator()
  {
  }

  @Override
  public void beginWindow(long l)
  {
    windowCount = 0;
  }

  @Override
  public void emitTuples()
  {
    for(; windowCount < numTuplesPerWindow; windowCount++)
    {
      String type = NODE_TYPE.get(rand.nextInt(3));
      String name = NODE_NAME.get(rand.nextInt(3));
      long timeStamp = System.currentTimeMillis();

      output.emit(new DataTuple(timeStamp, name, type));
    }
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext cntxt)
  {
  }

  @Override
  public void teardown()
  {
  }

  /**
   * @return the numTuplesPerWindow
   */
  public int getNumTuplesPerWindow()
  {
    return numTuplesPerWindow;
  }

  /**
   * @param numTuplesPerWindow the numTuplesPerWindow to set
   */
  public void setNumTuplesPerWindow(int numTuplesPerWindow)
  {
    this.numTuplesPerWindow = numTuplesPerWindow;
  }
}
