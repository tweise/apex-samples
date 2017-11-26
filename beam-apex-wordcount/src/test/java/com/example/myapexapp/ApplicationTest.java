/**
 * Put your copyright and license info here.
 */
package com.example.myapexapp;

import java.io.File;
import java.util.ServiceLoader;

import org.junit.Assert;
import org.apache.apex.api.EmbeddedAppLauncher;
import org.apache.apex.api.Launcher;
import org.apache.apex.api.Launcher.AppHandle;
import org.apache.apex.api.Launcher.LaunchMode;
import org.apache.beam.runners.apex.TestApexRunner;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.FileSystemRegistrar;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemRegistrar;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.datatorrent.api.Attribute;
import com.example.myapexapp.Application;
import com.example.myapexapp.Application.WordCountOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class ApplicationTest {

  @Test
  public void testApplication() throws Exception {

    HadoopFileSystemOptions fsoptions = PipelineOptionsFactory.as(HadoopFileSystemOptions.class);
    fsoptions.setHdfsConfiguration(ImmutableList.of(new Configuration()));
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

    String outputFilePrefix = "target/wordcountresult.txt";
    File outFile1 = new File(outputFilePrefix + "-00000-of-00002");
    File outFile2 = new File(outputFilePrefix + "-00001-of-00002");
    Assert.assertTrue(!outFile1.exists() || outFile1.delete());
    Assert.assertTrue(!outFile2.exists() || outFile2.delete());

    EmbeddedAppLauncher<?> launcher = Launcher.getLauncher(LaunchMode.EMBEDDED);
    Attribute.AttributeMap launchAttributes = new Attribute.AttributeMap.DefaultAttributeMap();

    Configuration conf = new Configuration(false);
    conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));

    PipelineOptionsFactory.register(WordCountOptions.class);
    WordCountOptions options = TestPipeline.testingPipelineOptions().as(WordCountOptions.class);
    options.setRunner(TestApexRunner.class);
    options.setInputFile(new File("./pom.xml").getAbsolutePath());
    options.setOutput(outputFilePrefix);
    // convert the options to command line string and pass it through the conf
    conf.set(Application.KEY_PIPELINE_OPTIONS, StringUtils.join(TestPipeline.convertToArgs(options), " "));
    AppHandle appHandle = launcher.launchApp(new Application(), conf, launchAttributes);

    Assert.assertTrue(appHandle.isFinished());
    Assert.assertTrue("result files exist", outFile1.exists() && outFile2.exists());
  }

}
