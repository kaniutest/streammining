# Tweetminning #
This is a twitter streamer using spark and kafka cache to perform sentimental analysis

## Getting started ##

Get and compile the application

## Running configurations ##

When running any of the spark jobs add system variables to command for twitter authentication i.e.

```
  -Dtwitter4j.oauth.consumerKey=consumerKey
  -Dtwitter4j.oauth.consumerSecret=consumerSecret
  -Dtwitter4j.oauth.accessToken=accessToken
  -Dtwitter4j.oauth.accessTokenSecret=accessTokenSecret
```

The values are generated by twitter using your twitter account.

## Authors

*  **Kaniu N.**

## Links

#### NLP
[[https://stanfordnlp.github.io/CoreNLP/]]

#### My ES instance: 
[[https://92s4zak4sq:uk1wjyly3g@first-cluster-5923838660.us-west-2.bonsaisearch.net]]

#### Docker Containers pulls
[[https://hub.docker.com/r/blacktop/kafka/]]
