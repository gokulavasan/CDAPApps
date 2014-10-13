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

import co.cask.cdap.api.annotation.Tick;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * Gets Real-time Twitter Stream, extracts HashTags and emits it.
 */
public class TweetStream extends AbstractFlowlet {
  public static Logger LOG = LoggerFactory.getLogger(TweetStream.class);

  private OutputEmitter<String> tweetText;
  private StatusListener statusListener;
  private TwitterStream tStream;
  private ConfigurationBuilder cb;
  private Queue<String> tweetQ = new ConcurrentLinkedQueue<String>();

  @Override
  public void initialize(FlowletContext context) {
    statusListener = new StatusListener() {
      @Override
      public void onStatus(Status status) {
        for (HashtagEntity hash : status.getHashtagEntities()) {
          tweetQ.add(hash.getText());
        }
      }

      @Override
      public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

      }

      @Override
      public void onTrackLimitationNotice(int i) {

      }

      @Override
      public void onScrubGeo(long l, long l2) {

      }

      @Override
      public void onStallWarning(StallWarning stallWarning) {

      }

      @Override
      public void onException(Exception e) {
        LOG.error(e.getMessage());
      }
    };

    cb = new ConfigurationBuilder();
    cb.setDebugEnabled(false)
      //Provide Twitter Credentials through Runtime Arguments.
      .setOAuthConsumerKey(context.getRuntimeArguments().get("ConsumerKey"))
      .setOAuthConsumerSecret(context.getRuntimeArguments().get("ConsumerSecret"))
      .setOAuthAccessToken(context.getRuntimeArguments().get("AccessToken"))
      .setOAuthAccessTokenSecret(context.getRuntimeArguments().get("AccessTokenSecret"));

    tStream = new TwitterStreamFactory(cb.build()).getInstance();
    //For testing, we can disable Twitter Stream and use testStream to send sample hashTags.
    if (!context.getRuntimeArguments().containsKey("disableLiveStream")) {
      tStream.addListener(statusListener);
      tStream.sample();
    }
  }

  @Tick(delay = 1L, unit = TimeUnit.MICROSECONDS)
  public void process() throws TwitterException, InterruptedException {
    if (!tweetQ.isEmpty()) {
      String tweet = tweetQ.remove();
      procMethod(tweet);
    }
  }

  public void procMethod(String tweet) {
    tweetText.emit(tweet, "wordHash", tweet);
  }
}

