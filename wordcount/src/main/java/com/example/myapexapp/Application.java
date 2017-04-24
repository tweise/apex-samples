/**
 * Put your copyright and license info here.
 */
package com.example.myapexapp;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;
import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.converter.Converter;

@ApplicationAnnotation(name="MyFirstApplication")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    LineByLineFileInputOperator lineReader = dag.addOperator("input",
        new LineByLineFileInputOperator());
    LineSplitter parser = dag.addOperator("parser", new LineSplitter());
    UniqueCounter counter = dag.addOperator("counter", new UniqueCounter());
    GenericFileOutputOperator<Object> output = dag.addOperator("output",
        new GenericFileOutputOperator<>());
    output.setConverter(new ToStringConverter());
    dag.addStream("lines", lineReader.output, parser.input);
    dag.addStream("words", parser.output, counter.data);
    dag.addStream("counts", counter.count, output.input);
  }

  public static class ToStringConverter implements Converter<Object, byte[]>
  {
    @Override
    public byte[] convert(Object tuple)
    {
      return tuple.toString().getBytes();
    }
  }

}
