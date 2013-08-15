# Machine learning for Traffic predictation

This is an implementation of traffic predication on dodger game night
using machine learning with elasticsearch.

Zachary Tong illustrated how to use elasticsearch to parse logs in elasticsearch and generate features for machine learning. His implementation originally was using PHP. I thought it would be fun to implement it with clojure.

See Zachary Tong's blog for detailed information.
  
  http://euphonious-intuition.com/2013/04/not-just-for-search-using-elasticsearch-with-machine-learning-algorithms/

## ElasticSearch date histogram facet.

Date histogram facet query is used to generate feature data for each event. Date histogram facet query is great; however, the value field must be numeric. There are plugins that allows histogram facet query on non-numeric field. The long term solution is use histogram Aggregator in Aggregation Module, which will be duel with ElasticSearch 1.0 soon. (can't wait for that).
  
  https://github.com/elasticsearch/elasticsearch/issues/3300

## Machine learning

Vowpal Warbit is used to line regression the data and find the pattern.

## Data

Please download traffic and dodger event raw data from UCI machine learning repo.
  
  http://archive.ics.uci.edu/ml/machine-learning-databases/event-detection/

## Installation and Usage
  lein deps
  
  lein compile

  lein-2 run index data/train.data

  lein-2 run gen-feature "9/28/2005 14:00"

  lein-2 run train data/train.data data/Dodgers.events output.model

  train predict plot

## Vowpal Wabbit
  1. generate data model with train data.
    -b [ --bit_precision ] arg  number of bits in the feature table
    increase to reduce collisions from hashing
    ./vw -d data/train.real -b 21 -c -f model.real

  2. fire up VW in deamon mode with data model, and stream data to it.
    ./vw --daemon --num_children 10 -i model.real

  3. feature generation
    Throw a ton of features to the model and let L1 sparsity to figure it out.

    Feature representation is a crucial machine learning design decision. 
    They cast a very wide net in terms of representing an ad including words and topics used in the ad, links to and from the ad landing page, information about the advertiser, and more. Ultimately they rely on strong L1 regularization to enforce sparsity and uncover a limited number of truly relevant features.

  