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
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.TimeseriesTable;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Flowlet that increments the count each hour of a HashTag in tagCount table and computes the
 * weighted score for that tag (Score = C(t) + C(t-1)/2 + C(t-2)/4 + ... )
 * where C(N) represents count of the tag at Nth hour.
 */
public class IncrementCounter extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(IncrementCounter.class);
  private static final Long MSIN1HR = TimeUnit.HOURS.toMillis(1);
  //Consider past HISTORY hours of counts for computing the score for each HashTag.
  private static final Long HISTORY = 12L;
  private Metrics tagMetrics;
  private OutputEmitter<PrefixData> tag;

  @UseDataSet("tagCount")
  TimeseriesTable table;

  @HashPartition("wordHash")
  @ProcessInput
  public void process(String hashtag) {
    processTag(hashtag);
    tagMetrics.count("tags", 1);
  }

  @ProcessInput
  public void process(StreamEvent event) {
    String hashtag = Charset.forName("UTF-8").decode(event.getBody()).toString();
    processTag(hashtag);
    tagMetrics.count("tags", 1);
  }

  //Increment Count
  private void processTag(String hashtag) {
    Long hourCount = System.currentTimeMillis() / MSIN1HR;
    Iterator<TimeseriesTable.Entry> row = table.read(Bytes.toBytes(hashtag), hourCount, hourCount);
    if (row != null && row.hasNext()) {
      //Count exists for this hour!
      Long value = 0L;
      while (row.hasNext()) {
        value = Bytes.toLong(row.next().getValue());
      }
      value++;
      TimeseriesTable.Entry entry = new TimeseriesTable.Entry(Bytes.toBytes(hashtag), Bytes.toBytes(value), hourCount);
      table.write(entry);
    } else {
      //Create a new entry
      TimeseriesTable.Entry entry = new TimeseriesTable.Entry(Bytes.toBytes(hashtag), Bytes.toBytes(1L), hourCount);
      table.write(entry);
    }

    Iterator<TimeseriesTable.Entry> timeCols = table.read(Bytes.toBytes(hashtag), hourCount - HISTORY, hourCount);
    TreeMap<Long, Long> hourValue = new TreeMap<Long, Long>();
    while (timeCols.hasNext()) {
      TimeseriesTable.Entry e = timeCols.next();
      hourValue.put(e.getTimestamp(), Bytes.toLong(e.getValue()));
    }

    //Compute Score!
    Double score = 0D;
    for (Long hour : hourValue.descendingKeySet()) {
      Long power = ((hourCount - hour) >= 0) ? (hourCount - hour) : 0;
      Double exponent = Math.pow(2, -power);
      score += (exponent * hourValue.get(hour));
    }

    PrefixData data = new PrefixData(hashtag, score);
    tag.emit(data, "tagHash", data.getHashtag().hashCode());
  }
}
