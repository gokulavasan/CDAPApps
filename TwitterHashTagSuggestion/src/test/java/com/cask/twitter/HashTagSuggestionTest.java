/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.cask.twitter;

import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ProcedureManager;
import co.cask.cdap.test.RuntimeMetrics;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Test for {@link HashTagApp}.
 */
public class HashTagSuggestionTest extends TestBase {

  @Test
  public void test() throws TimeoutException, InterruptedException, IOException {
    // Deploy the HashTagSuggestion application
    ApplicationManager appManager = deployApplication(HashTagApp.class);
    Map<String, String> flowRuntimArgs = new HashMap<String, String>();
    flowRuntimArgs.put("disableLiveStream", "");

    // Start TwitterFlow (Live Twitter Stream disabled)
    FlowManager flowManager = appManager.startFlow("TwitterFlow", flowRuntimArgs);

    //Send dummy hashtags
    StreamWriter streamWriter = appManager.getStreamWriter("testStream");
    //Number of prefixes generated = length(Hello) + length(Hello) + length(Hola) + length(CDAP) + length(Tigon) = 23
    streamWriter.send("Hello");
    streamWriter.send("Hello");
    streamWriter.send("Hola");
    streamWriter.send("CDAP");
    streamWriter.send("Tigon");

    try {
      // Wait for the last Flowlet processing 23 events (total prefixes generated), or at most 30 seconds
      RuntimeMetrics metrics = RuntimeStats.getFlowletMetrics("HashTagApp", "TwitterFlow", "updateReco");
      metrics.waitForProcessed(23, 30, TimeUnit.SECONDS);
    } finally {
      flowManager.stop();
    }

    // Start RetrieveSuggestions Procedure and query it
    ProcedureManager procedureManager = appManager.startProcedure("GetSuggestions");
    Gson gson = new Gson();
    String response;
    Map<String, Double> responseMap = new HashMap<String, Double>();
    Map<String, String> prefix = new HashMap<String, String>();

    prefix.put("prefix", "He");
    response = procedureManager.getClient().query("recommend", prefix);
    response = response.substring(1, response.length() - 1).replaceAll("\\\\", "");
    responseMap = (Map<String, Double>) gson.fromJson(response, responseMap.getClass());
    Assert.assertEquals(1, responseMap.size());
    Assert.assertEquals(2.0, responseMap.get("Hello"), 0.001);

    prefix.put("prefix", "H");
    response = procedureManager.getClient().query("recommend", prefix);
    response = response.substring(1, response.length() - 1).replaceAll("\\\\", "");
    responseMap = (Map<String, Double>) gson.fromJson(response, responseMap.getClass());
    Assert.assertEquals(2, responseMap.size());
    Assert.assertEquals(1.0, responseMap.get("Hola"), 0.001);

    appManager.stopAll();
  }
}
