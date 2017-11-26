/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example.myapexapp;

import org.apache.apex.malhar.lib.utils.FileContextUtils;
import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.runners.apex.ApexRunner;
import org.apache.beam.runners.apex.translation.ApexPipelineTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import java.lang.reflect.Method;
import java.util.List;

/**
 * ApexRunner doesn't implement launch on YARN yet, hence we move the Beam
 * pipeline code into {@link StreamingApplication} to translate it into the Apex
 * DAG and then launch the application using apex CLI. Later, instead of
 * implementing {@link StreamingApplication}, the main method will call the
 * runner.
 */
@ApplicationAnnotation(name="BeamWordCountApplication")
public class Application implements StreamingApplication
{
  public static final String KEY_PIPELINE_OPTIONS = "pipelineOptions";

  /**
   * Concept #2: You can make your pipeline assembly code less verbose by defining your DoFns
   * statically out-of-line. This DoFn tokenizes lines of text into individual words; we pass it
   * to a ParDo in the pipeline.
   */
  static class ExtractWordsFn extends DoFn<String, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      // Split the line into words.
      String[] words = c.element().split("[^a-zA-Z']+");

      // Output each word encountered into the output PCollection.
      for (String word : words) {
        if (!word.isEmpty()) {
          c.output(word);
        }
      }
    }
  }

  /**
   * A simple function that outputs the value
   *
   */

  static class ExtractString extends DoFn<KV<LongWritable, Text>, String>
  {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception
    {
      c.output(c.element().getValue().toString());
    }
  }

  /** A SimpleFunction that converts a Word and Count into a printable string. */
  public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
    @Override
    public String apply(KV<String, Long> input) {
      return input.getKey() + ": " + input.getValue();
    }
  }

  /**
   * A PTransform that converts a PCollection containing lines of text into a PCollection of
   * formatted word counts.
   *
   * <p>Concept #3: This is a custom composite transform that bundles two transforms (ParDo and
   * Count) as a reusable PTransform subclass. Using composite transforms allows for easy reuse,
   * modular testing, and an improved monitoring experience.
   */
  public static class CountWords extends PTransform<PCollection<String>,
      PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

      // Convert lines of text into individual words.
      PCollection<String> words = lines.apply(
          ParDo.of(new ExtractWordsFn()));

      // Count the number of times each word occurs.
      PCollection<KV<String, Long>> wordCounts =
          words.apply(Count.<String>perElement());

      return wordCounts;
    }
  }

  /**
   * Options supported by {@link WordCount}.
   *
   * <p>Concept #4: Defining your own configuration options. Here, you can add your own arguments
   * to be processed by the command-line parser, and specify default values for them. You can then
   * access the options values in your pipeline code.
   *
   * <p>Inherits standard configuration options.
   */
  public interface WordCountOptions extends PipelineOptions {

    /**
     * By default, this example reads from /tmp/input/* from your HDFS.
     * Set this option to choose a different location.
     */
    @Description("Path of the file to read from")
    @Default.String("/tmp/input/*")
    String getInputFile();
    void setInputFile(String value);

    /**
     * Set this option to specify where to write the output, default is /tmp/output.
     * The output directory should not exist on HDFS.
     */
    @Description("Path of the file to write to")
    @Required
    @Default.String("/tmp/output/")
    String getOutput();
    void setOutput(String value);
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
/*
    HadoopFileSystemOptions fsoptions = PipelineOptionsFactory.as(HadoopFileSystemOptions.class);
    //fsoptions.setHdfsConfiguration(ImmutableList.of(new Configuration()));
    for (FileSystemRegistrar registrar
        : Lists.newArrayList(ServiceLoader.load(FileSystemRegistrar.class).iterator())) {
      System.out.println(registrar);
      if (registrar instanceof HadoopFileSystemRegistrar) {
        Iterable<FileSystem> fileSystems = registrar.fromOptions(fsoptions);
        for (FileSystem fs : fileSystems) {
          System.out.println(fs);
        }
      }
    }
*/
    String optionsStr = conf.get(KEY_PIPELINE_OPTIONS, "--runner=ApexRunner");
    String[] args = StringUtils.splitByWholeSeparator(optionsStr, " ");
    WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(WordCountOptions.class);
    Pipeline p = Pipeline.create(options);
    // Concepts #2 and #3: Our pipeline applies the composite CountWords transform, and passes the
    // static FormatAsTextFn() to the ParDo transform.
      p.apply("ReadFromHDFS", TextIO.read().from(absoluteUri(options.getInputFile())))
      .apply(new CountWords())
      .apply(MapElements.via(new FormatAsTextFn()))
      .apply("WriteToHDFS", TextIO.write().to(absoluteUri(options.getOutput())).withNumShards(2));

    ApexPipelineOptions apexPipelineOptions =
        PipelineOptionsValidator.validate(ApexPipelineOptions.class, options);
    final ApexPipelineTranslator translator = new ApexPipelineTranslator(apexPipelineOptions);

    // roundabout way to apply overrides - we just want to translate, not run the pipeline here
    ApexRunner runner = new ApexRunner(apexPipelineOptions);
    try {
      Method m = ApexRunner.class.getDeclaredMethod("getOverrides");
      m.setAccessible(true);
      @SuppressWarnings("unchecked")
      List<PTransformOverride> overrides = (List<PTransformOverride>)m.invoke(runner);
      p.replaceAll(overrides);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    translator.translate(p, dag);
  }

  private String absoluteUri(String path) {
    Path p = new Path(path);
    if (!p.toUri().isAbsolute()) {
      try {
        FileContext fc = FileContextUtils.getFileContext(p);
        // TextIO.Write.to has trouble with absolute URI for local FS...
        if (!"file".equals(fc.getDefaultFileSystem().getUri().getScheme())) {
          p = p.makeQualified(fc.getDefaultFileSystem().getUri(), fc.getWorkingDirectory());
          System.out.println(p);
          return p.toString();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return path;
  }

}
