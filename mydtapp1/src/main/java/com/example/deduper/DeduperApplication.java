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

import java.util.Random;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name = "DeduperApplication")
public class DeduperApplication implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    RandomMessageGenerator randomMessageGenerator = dag.addOperator("Random Generator", new RandomMessageGenerator());
    DeduperStaticImpl deduper = dag.addOperator("Deduper", new DeduperStaticImpl());
    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());

    dag.addStream("data", randomMessageGenerator.messages, deduper.input);
    dag.addStream("deduped", deduper.output, console.input);
  }

  public static class RandomMessageGenerator extends BaseOperator implements InputOperator
  {
    private int numTuplesPerWindow = 100;

    private transient Random random = new Random();
    private transient boolean emit;

    public final transient DefaultOutputPort<Message> messages = new DefaultOutputPort<>();

    @Override
    public void beginWindow(long windowId)
    {
      emit = true;
    }

    @Override
    public void emitTuples()
    {
      if (emit) {
        emit = false;
        for (int i = 0; i < numTuplesPerWindow; i++) {
          messages.emit(new Message(Integer.toString(random.nextInt(10)), System.currentTimeMillis()));

        }
      }
    }

    public int getNumTuplesPerWindow()
    {
      return numTuplesPerWindow;
    }

    public void setNumTuplesPerWindow(int numTuplesPerWindow)
    {
      this.numTuplesPerWindow = numTuplesPerWindow;
    }
  }
}
