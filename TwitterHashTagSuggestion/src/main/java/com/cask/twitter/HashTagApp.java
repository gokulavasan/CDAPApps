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

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.TimeseriesTable;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureResponse;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * HashTagApp - Provides realtime Twitter HashTag suggestions based on a HashTag prefix.
 * <uL>
 *   <li>A Flow that reads the stream or gets live data from twitter, process it and stores it in DataSets</li>
 *   <li>A TimeSeriesTable to store the count of an HashTag per hour</li>
 *   <li>A KeyValueTable to store the suggestions for each prefix</li>
 *   <li>A Procedure that is used to retrieve hashtag suggestions given a tag prefix</li>
 *   <li>A Stream to send dummy hashtags for testing</li>
 * </uL>
 */
public class HashTagApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("HashTagApp");
    setDescription("Realtime Suggestion of Twitter HashTags");
    createDataset("tagCount", TimeseriesTable.class);
    createDataset("suggestMap", KeyValueTable.class);
    addFlow(new TweetFlow());
    addProcedure(new GetSuggestions());
    addStream(new Stream("testStream"));
  }

  public static class TweetFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("TwitterFlow")
        .setDescription("Gets Realtime Tweets, extracts and processes HashTags")
        .withFlowlets()
          .add("liveTweet", new TweetStream())
          .add("counterInc", new IncrementCounter())
          .add("prefixGen", new PrefixGenerator())
          .add("updateReco", new RecoUpdater(), 3)
        .connect()
          .from("liveTweet").to("counterInc")
          .fromStream("testStream").to("counterInc")
          .from("counterInc").to("prefixGen")
          .from("prefixGen").to("updateReco")
        .build();
    }
  }

  //Exposes "recommend" Handle - Given a prefix, returns HashTag suggestions for that prefix with corresponding scores.
  public static class GetSuggestions extends AbstractProcedure {
    private static final Gson GSON = new Gson();
    private static final Logger LOG = LoggerFactory.getLogger(GetSuggestions.class);

    @UseDataSet("suggestMap")
    KeyValueTable table;

    @Handle("recommend")
    public void recommend(ProcedureRequest request, ProcedureResponder responder) throws IOException {
      String hashTagPrefix = request.getArgument("prefix");
      if (hashTagPrefix == null) {
        responder.error(ProcedureResponse.Code.CLIENT_ERROR, "Provide a prefix argument");
        return;
      }

      responder.sendJson(ProcedureResponse.Code.SUCCESS, GSON.toJson(
        MapSerdesUtil.deserializeMap(table.read(hashTagPrefix.toLowerCase()))));
    }
  }
}
