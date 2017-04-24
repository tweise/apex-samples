/**
 * Put your copyright and license info here.
 */
package com.example.myapexapp;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.api.EmbeddedAppLauncher;
import org.apache.apex.api.Launcher;
import org.apache.apex.api.Launcher.AppHandle;
import org.apache.apex.api.Launcher.LaunchMode;
import org.apache.apex.api.Launcher.ShutdownMode;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Attribute;

/**
 * Test the DAG declaration in embedded mode.
 */
public class ApplicationTest {

  @Test
  public void testApplication() throws Exception {
    EmbeddedAppLauncher<?> launcher = Launcher.getLauncher(LaunchMode.EMBEDDED);
    Attribute.AttributeMap launchAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    launchAttributes.put(EmbeddedAppLauncher.RUN_ASYNC, true);
    //launchAttributes.put(EmbeddedAppLauncher.HEARTBEAT_MONITORING, false);
    Configuration conf = new Configuration(false);
    conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
    conf.addResource(this.getClass().getResourceAsStream("/wordcounttest-properties.xml"));

    File resultFile = new File("./target/wordcountresult_4.0");
    if (resultFile.exists() && !resultFile.delete()) {
      throw new AssertionError("Failed to delete " + resultFile);
    }
    AppHandle appHandle = launcher.launchApp(new Application(), conf, launchAttributes);
    long timeoutMillis = System.currentTimeMillis() + 10000;
    while (!appHandle.isFinished() && System.currentTimeMillis() < timeoutMillis) {
      Thread.sleep(500);
      if (resultFile.exists() && resultFile.length() > 0) {
        break;
      }
    }
    appHandle.shutdown(ShutdownMode.KILL);
    Assert.assertTrue(resultFile.exists() && resultFile.length() > 0);
    String result = FileUtils.readFileToString(resultFile);
    Assert.assertTrue(result, result.contains("MyFirstApplication=5"));
  }

}
