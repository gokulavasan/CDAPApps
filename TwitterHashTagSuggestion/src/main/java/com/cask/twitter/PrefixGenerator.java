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
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates and emits Prefixes for each received HashTag.
 * For example, Prefixes for "Hello" are { "H", "He", "Hel", "Hell", "Hello" }
 */
public class PrefixGenerator extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(PrefixGenerator.class);
  private OutputEmitter<PrefixData> prefixEmitter;

  @HashPartition("tagHash")
  @ProcessInput
  public void process(PrefixData tagScore) {
    String tag = tagScore.getHashtag();
    for (int i = 1; i <= tag.length(); i++) {
      PrefixData data = new PrefixData(tag.substring(0, i), tag, tagScore.getScore());
      prefixEmitter.emit(data, "prefixHash", data.getPrefix());
    }
  }
}
