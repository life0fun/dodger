(ns dodger.vwfeature
  (:require [clojure.string :as str])
  (:require [clojure.java.jdbc :as sql])
  (:import [java.io FileReader]
           [java.util Map Map$Entry List ArrayList Collection Iterator HashMap])
  (:require [clj-redis.client :as redis])    ; bring in redis namespace
  (:require [clojure.data.json :as json])
  (:require [clojure.pprint :as pp])
  (:require [clj-time.core :as clj-time :exclude [extend]]
            [clj-time.format])
  (:require [dodger.incanter.plot :refer :all]))

;
; vw input format 
;  https://github.com/JohnLangford/vowpal_wabbit/wiki/Input-format
;  The raw (plain text) input data for VW should have one example per line. 
;
; [Label] [Importance [Tag]]|Namespace Features |Namespace Features ... |Namespace Features
;
; Namespace=String[:Value] is an identifier of a source of information for the example 
; optionally followed by a float (e.g., MetricFeatures:3.28), which acts as a global scaling 
; of all the values of the features in this namespace. 
;
; Features=(String[:Value] )*  is a sequence of whitespace separated strings, each of which is optionally followed by a float
;
; 1 1.0 |MetricFeatures:3.28 height:1.5 length:2.0 |Says black with white stripes |OtherFeatures NumberOfLegs:4.0 HasStripes
;
; 1 1.0 zebra|MetricFeatures:3.28 height:1.5 length:2.0 |Says black with white stripes |OtherFeatures NumberOfLegs:4.0 HasStripes
;

; the stats on values inside each bucket
(def stats [:total :min :max :mean])

; for each value map at idx, gen a feature:value pair
; valmap {:time 1127928600000, :count 6, :min 32.0, :max 35.0, :total 200.0,:total_count 6,:mean 33.333333333333336}
(defn one-pair 
  "given a value map and the idx of this value map in bucket's value ary, ret a 
  value pair string, note we need to extract stats fields and add log features, etc."
  [bucket idx valmap] 
  (letfn [(stats-cat [bucket]
            (if (re-find #"-5m$" (str bucket))
              (vector (first stats))    ; for 5m bucket, the base, single value of total. no others to mean.
              stats))]  ; for other buckets, get stats of tot min max mean.
    ; use loop so to carry intermediate result
    (loop [stats (stats-cat bucket) ; stats [:total :min ...]
           fpairs []]
      (if (nil? stats)
        (clojure.string/join " " fpairs)  ; concat array to one string
        (let [st (first stats) 
              fname (clojure.string/join "_" (map str (vector bucket (name st) idx)))
              fpair (str fname ":" (st valmap))]
          (recur (next stats) (conj fpairs fpair)))))))

; generate feature row for data in a bucket. for 2 hours span, in 1 hour bucket,
; we have 2 buckets, each groups 12 values of datapoint sampled every 5 min.
; we two feature value pairs.
(defn bucket-feature-pairs
  "generate all features by value list within one bucket"
  [bucket values]
  (let [idx-val (map-indexed vector values)  ; idx of valmap is part of feature name
        pairs (map (fn [[idx valmap]] (one-pair bucket idx valmap)) idx-val)]
    ; pairs is a list contains f:v pairs across all values and stats in this bucket.
    (prn "bucket :" bucket pairs)
    (str pairs)))   ; force eval of lazy map

(defn vw-feature-data-format
  "convert es facets result into vm feature format "
  [facets-data]
  (let [buckets (keys facets-data)
        values (vals facets-data)]
    ;(pp/pprint  buckets)
    ;(pp/pprint values)
    (loop [buckets (keys facets-data) bktfpairs []]
      (if (nil? buckets)
        (clojure.string/join " " bktfpairs)
        (let [bkt (first buckets)
              entries (:entries (bkt facets-data))
              bktpair (bucket-feature-pairs (name bkt) entries)]
          (recur (next buckets) (conj bktfpairs bktpair)))))))
