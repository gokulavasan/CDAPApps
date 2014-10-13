# Twitter HashTag Suggestion CDAP Application

This is a CDAP Application that uses realtime twitter feed to suggest hashtags to users given a tag prefix.

Given a prefix, say "Ki" it returns the hashtag suggestions, for example, Kids, Kite, Kick, Kiosk, Kind. 
The suggestions are kept up-to-date with the realtime stream.

Note : If you have not downloaded CDAP, you do so by visiting : [CDAP Downloads](http://cask.co/downloads)

You can build this CDAP Application by executing in the current directory :
```mvn clean package``` 

You can then deploy the Application JAR (found in the target directory) on to CDAP. It might be useful to review the
[CDAP UI/Console documentation](http://docs.cask.co/cdap/current/en/admin.html#cdap-console)

The TwitterFlow Flow in this Application needs the following four runtime arguments to pull in realtime Twitter feed.

- ```ConsumerKey```
- ```ConsumerSecret```
- ```AccessToken```
- ```AccessTokenSecret```

Follow the steps at [Twitter oauth access tokens](https://dev.twitter.com/oauth/overview/application-owner-access-tokens) to obtain the above credentials.

When the application is deployed on to CDAP, these runtime args need to be specified before/while starting the flow.

Once the Flow has started running, you can visit the Procedures page and start the GetSuggestions Procedure.

In order to get recommendations for a given hashtag prefix, you can enter "recommend" in the method text box and in the
parameters you can specify the prefix in this format : {"prefix":"Ki"}

Press Execute and then you should be able to see the suggestions below. Scores associated with each suggested hashtag 
are also displayed.

The above operations, that is, deploying an application, starting flow with runtime args, 
starting and querying a procedure can also be executed through HTTP calls.

Visit [HTTP RESTful API](http://docs.cask.co/cdap/current/en/api.html#http-restful-api) for more info.

Follow the latest on cdap on irc freenode (#cdap channel) and cdap-user@googlegroups.com.

## License and Trademarks

Copyright 2014 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
