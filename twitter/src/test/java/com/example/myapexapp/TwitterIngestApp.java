package com.example.myapexapp;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.concurrent.ArrayBlockingQueue;

import org.junit.Test;

import org.apache.apex.api.EmbeddedAppLauncher;
import org.apache.apex.api.Launcher;
import org.apache.apex.api.Launcher.LaunchMode;
import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.contrib.twitter.TwitterSampleInput;

import jline.internal.InputStreamReader;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;
import twitter4j.conf.ConfigurationBuilder;

import static org.apache.apex.malhar.stream.api.Option.Options.name;

public class TwitterIngestApp
{
  /**
   * Check whether every character in a string is ASCII encoding.
   */
  public static class StringUtils
  {
    static CharsetEncoder encoder = Charset.forName("US-ASCII").newEncoder();

    public static boolean isAscii(String v)
    {
      return encoder.canEncode(v);
    }
  }

  public static class Collector extends BaseOperator
  {
    private String path;
    private PrintWriter pw;

    @Override
    public void setup(Context.OperatorContext context)
    {
      Preconditions.checkNotNull(path, "path cannot be null");
      super.setup(context);
      try {
        pw = new PrintWriter(new FileWriter(path));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void teardown()
    {
      pw.close();
      super.teardown();
    }

    public final transient DefaultInputPort<String> input = new DefaultInputPort<String>()
    {
      @Override
      public void process(String tuple)
      {
        pw.write(tuple + "\n");
      }
    };
  }

  public static class TwitterSampleInputWithJson extends TwitterSampleInput
  {
    public final transient DefaultOutputPort<String> json = new DefaultOutputPort<>();

    private transient ArrayBlockingQueue<String> jsonStatusQueue = new ArrayBlockingQueue<>(1024 * 1024);

    @Override
    protected ConfigurationBuilder setupConfigurationBuilder()
    {
      ConfigurationBuilder cb = super.setupConfigurationBuilder();
      cb.setJSONStoreEnabled(true);
      return cb;
    }

    @Override
    public void onStatus(Status status)
    {
      super.onStatus(status);
      // needs to occur in same thread
      String json = TwitterObjectFactory.getRawJSON(status);
      if (json == null) {
        throw new NullPointerException("got null json status");
      }
      jsonStatusQueue.add(json);
    }

    @Override
    public void emitTuples()
    {
      for (int size = jsonStatusQueue.size(); size-- > 0;) {
        String jsonStatus = jsonStatusQueue.poll();

        if (json.isConnected()) {
          json.emit(jsonStatus);
        }
      }
      super.emitTuples();
    }

  }

  static class ASCIIFilter implements Function.FilterFunction<String>
  {
    @Override
    public boolean f(String input)
    {
      return StringUtils.isAscii(input);
    }
  }

  //@Test
  public void testFile() throws Exception
  {
    FileInputStream fis = new FileInputStream("./target/sampleTweets.txt");
    //Construct BufferedReader from InputStreamReader
    BufferedReader br = new BufferedReader(new InputStreamReader(fis));
    String line = null;
    int lineNum = 0;
    while ((line = br.readLine()) != null) {
      lineNum++;
      System.out.println(line);
      try {
        TwitterObjectFactory.createStatus(line);
      } catch (Exception e) {
        System.out.println("Error line " + lineNum + ": " + line);
        throw new RuntimeException(e);
      }
    }
    br.close();
  }

  @Test
  public void twitterIngest() throws Exception
  {
    TwitterSampleInputWithJson input = new TwitterSampleInputWithJson();
    Collector collector = new Collector();
    collector.path = "./target/sampleTweets.txt";

    final ApexStream<Object> tags = StreamFactory.fromInput(input, input.json, name("twitterSampleInput"))
        .filter(new ASCIIFilter())
        .filter(new Function.FilterFunction<String>()
        {
          @Override
          public boolean f(String input)
          {
            return !input.contains("\n");
          }
        })
        .map(new Function.MapFunction<String, String>()
        {
          @Override
          public String f(String input)
          {
            return input;
          }
        })
        .print()
        .endWith(collector, collector.input);


    EmbeddedAppLauncher<?> launcher = Launcher.getLauncher(LaunchMode.EMBEDDED);
    Attribute.AttributeMap launchAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    launchAttributes.put(EmbeddedAppLauncher.RUN_ASYNC, false);
    launchAttributes.put(EmbeddedAppLauncher.RUN_MILLIS, 60000L);
    //launchAttributes.put(EmbeddedAppLauncher.HEARTBEAT_MONITORING, false);
    Configuration conf = new Configuration(false);
    conf.addResource(new Path(TwitterStatsAppTest.TEMPLATE_PROPERTIES_PATH));
    conf.addResource(new Path(TwitterStatsAppTest.PROPERTIES_PATH));
    //conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));

    StreamingApplication app = new StreamingApplication() {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
        tags.populateDag(dag);
      }
    };
    launcher.launchApp(app, conf, launchAttributes);
  }
}
