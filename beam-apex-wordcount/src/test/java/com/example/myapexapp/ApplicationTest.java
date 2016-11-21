/**
 * Put your copyright and license info here.
 */
package com.example.myapexapp;

import java.io.File;
import java.io.IOException;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.apache.beam.runners.apex.TestApexRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.datatorrent.api.LocalMode;
import com.example.myapexapp.Application;
import com.example.myapexapp.Application.WordCountOptions;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest {

  @Test
  public void testApplication() throws IOException, Exception {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));

      PipelineOptionsFactory.register(WordCountOptions.class);
      WordCountOptions options = TestPipeline.testingPipelineOptions().as(WordCountOptions.class);
      options.setRunner(TestApexRunner.class);
      options.setInputFile(new File("./pom.xml").getAbsolutePath());
      String outputFilePrefix = "target/wordcountresult.txt";
      options.setOutput(outputFilePrefix);
      // convert the options to command line string and pass it through the conf
      conf.set(Application.KEY_PIPELINE_OPTIONS, StringUtils.join(TestPipeline.convertToArgs(options), " "));
      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(10000); // runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}
