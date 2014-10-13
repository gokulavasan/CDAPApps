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

/**
 * PrefixData - Data that is passed between Flowlets.
 */
public class PrefixData {
  private String prefix;
  private String hashtag;
  private Double score;

  public PrefixData(String prefix, String hashtag, Double score) {
    this.prefix = prefix;
    this.hashtag = hashtag;
    this.score = score;
  }

  public PrefixData(String hashtag, Double score) {
    this(null, hashtag, score);
  }

  public String getPrefix() {
    return prefix;
  }

  public String getHashtag() {
    return hashtag;
  }

  public Double getScore() {
    return score;
  }
}
