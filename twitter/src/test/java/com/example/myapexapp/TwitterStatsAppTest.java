package com.example.myapexapp;

import org.joda.time.Duration;
import org.junit.Test;

import org.apache.apex.api.EmbeddedAppLauncher;
import org.apache.apex.api.Launcher;
import org.apache.apex.api.Launcher.AppHandle;
import org.apache.apex.api.Launcher.LaunchMode;
import org.apache.apex.api.Launcher.ShutdownMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.lib.io.ConsoleOutputOperator;

import twitter4j.Status;

public class TwitterStatsAppTest
{
  public static final String TEMPLATE_PROPERTIES_PATH = "./twitterDemoProperties_template.xml";
  public static final String PROPERTIES_PATH = "./twitterDemoProperties.xml";
  private boolean defaultTwitterInput = false;
  private boolean defaultWebsocketOutput = false;

  @Test
  public void testApplication() throws Exception {
    EmbeddedAppLauncher<?> launcher = Launcher.getLauncher(LaunchMode.EMBEDDED);
    Attribute.AttributeMap launchAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    //launchAttributes.put(EmbeddedAppLauncher.RUN_ASYNC, false);
    //launchAttributes.put(EmbeddedAppLauncher.HEARTBEAT_MONITORING, false);
    Configuration conf = new Configuration(false);
    conf.addResource(new Path(TEMPLATE_PROPERTIES_PATH));
    conf.addResource(new Path(PROPERTIES_PATH));
    //conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
    conf.setBooleanIfUnset(TwitterStatsApp.IS_TWITTER_SAMPLE_INPUT, defaultTwitterInput);
    conf.setBooleanIfUnset(TwitterStatsApp.IS_WEBSOCKET_OUTPUT, defaultWebsocketOutput);

    StreamingApplication app = /*new TestApplication();*/ new TwitterStatsApp();
    AppHandle appHandle = launcher.launchApp(app, conf, launchAttributes);
    long timeoutMillis = System.currentTimeMillis() + Duration.standardMinutes(15).getMillis();
    while (!appHandle.isFinished() && System.currentTimeMillis() < timeoutMillis) {
      Thread.sleep(500);
    }
    appHandle.shutdown(ShutdownMode.KILL);
  }


  static class TestApplication implements StreamingApplication {
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      final OutputPort<Status> statusPort;

        TwitterSampleInput sampleInput = dag.addOperator("twitterSampleInput", new TwitterSampleInput());
        statusPort = sampleInput.status;
        ConsoleOutputOperator console = dag.addOperator("topNConsole", new ConsoleOutputOperator());
        //DevNullCounter<Object> console = dag.addOperator("devNull", new DevNullCounter());
        //console.setDebug(true);
        dag.addStream("tweets", statusPort, console.input).setLocality(Locality.CONTAINER_LOCAL);
    }

  }


}
