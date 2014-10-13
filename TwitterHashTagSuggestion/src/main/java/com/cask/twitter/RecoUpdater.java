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

import co.cask.cdap.api.annotation.HashPartition;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Updates the recommendation based on each received Prefix (based on the score).
 */
public class RecoUpdater extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(RecoUpdater.class);
  //Maximum HashTag Recommendations that we want to save per Prefix.
  private static final int MAX_RECOS = 5;

  @UseDataSet("suggestMap")
  KeyValueTable table;

  @HashPartition("prefixHash")
  @ProcessInput
  public void process(PrefixData prefixScore) {
    String prefix = prefixScore.getPrefix().toLowerCase();
    Map<String, Double> currentReco = MapSerdesUtil.deserializeMap(table.read(prefix));
    Map<String, Double> newReco = generateNewReco(currentReco, prefixScore.getHashtag(), prefixScore.getScore());
    //Save the new recommendation for this prefix.
    table.write(prefix, MapSerdesUtil.serializeMap(newReco));
  }

  @SuppressWarnings("unchecked")
  private Map<String, Double> generateNewReco(Map<String, Double> current, String hashTag, Double score) {
    //Add new hashtag to the reco list (if it exists already, its score will get updated).
    current.put(hashTag, score);
    if (current.size() < MAX_RECOS) {
      //If size of reco is less than MAX_RECOS, return it.
      return current;
    } else {
      //If size is more than MAX_RECOS, take the top MAX_RECOS records. This keeps the map size, per prefix, constant.
      Map<String, Double> sortedMap = MapSerdesUtil.sortByComparator(current);
      Map<String, Double> returnMap = Maps.newLinkedHashMap();
      for(Map.Entry<String, Double> entry : sortedMap.entrySet()) {
        if (returnMap.size() >= MAX_RECOS) {
          break;
        }
        returnMap.put(entry.getKey(), entry.getValue());
      }
      return returnMap;
    }
  }
}
