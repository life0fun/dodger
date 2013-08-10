(ns dodger.vwfeature
  (:import  [java.lang.Math]
            [java.io FileReader]
            [java.util Map Map$Entry List ArrayList Collection Iterator HashMap])
  (:require [clojure.string :as str]
            [clojure.data.json :as json]
            [clojure.pprint :as pp]
            [clojure.java.io :only [reader] :refer [reader]]
            [clojure.java.jdbc :as sql])
  (:require [clj-redis.client :as redis])    ; bring in redis namespace
  (:require [clj-time.core :as clj-time :exclude [extend]]
            [clj-time.format :refer [parse unparse formatter]]
            [clj-time.coerce :refer [to-long from-long]])
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

; for each value map at idx, gen all stats feature[max, min, etc] pairs.
; valmap {:time 1127928600000, :count 6, :min 32.0, :max 35.0, :total 200.0,:total_count 6,:mean 33.333333333333336}
(defn one-val-stats
  "given a value map and the idx of this value map in bucket's value ary, ret a 
  value pair string, note we need to extract stats fields and add log features, etc."
  [bucket idx valmap] 
  (letfn [(stats-cat [bucket]
            (if (re-find #"-5m$" (str bucket))
              (vector (first stats))    ; for 5m bucket, the base, single value of total. no others to mean.
              stats))  ; for other buckets, get stats of tot min max mean.
          ; get the base 2 log when val gt 0
          (get-log [val]
            (let [v (Math/abs val)]
              (if (= v 0)
                0
                (/ (Math/log v) (Math/log 2)))))
          ; get categorical feature value pair
          (categoric-feature [fname val]
            (let [logv (get-log val)
                  cats [fname 
                        (str fname "_log") 
                        (str fname "_" val)
                        (str fname "_" (Math/round val))]
                  vals [val logv 1 1]
                  fvals (map str cats (repeat 4 ":") vals)]
              (clojure.string/join " " fvals)))]

    ; use loop so to carry intermediate result
    (loop [stats (stats-cat bucket) ; stats [:total :min ...]
           statpairs []]  ; iterate all stats features [bucket_total_i:val, bucket_max_i:val]
      (if (nil? stats)
        (clojure.string/join " " statpairs)  ; concat array to one string
        (let [st (first stats) 
              fname (clojure.string/join "_" (map str (vector bucket (name st) idx)))
              val (st valmap)
              statpair (categoric-feature fname val)]
          (recur (next stats) (conj statpairs statpair)))))))

; generate feature row for data in a bucket. for 2 hours span, in 1 hour bucket,
; we have 2 buckets, each groups 12 values of datapoint sampled every 5 min.
; we two feature value pairs.
(defn bucket-vals-stats
  "generate all features by value list within one bucket"
  [bucket values]
  (let [idx-val (map-indexed vector values)  ; idx of valmap is part of feature name
        bkt-stats (map (fn [[idx valmap]] (one-val-stats bucket idx valmap)) idx-val)
        bkt-stats-str (clojure.string/join " " bkt-stats)] ; join a list of string vals to one big string
    ; bkt-stats is a list contains pairs across all values over stats in this bucket.
    ;(prn "bucket :" bucket bkt-stats-str)
    bkt-stats-str))  ; all vals over stats for this bucket 


; convert facets bucket histogram into a big vw feature string
(defn vw-feature-data-format
  "convert es facets result into vm feature format, ret a big string with all features"
  [facets-data]
  (let [buckets (keys facets-data)
        values (vals facets-data)]
    (loop [buckets (keys facets-data) bktsfeatures []]  ; 
      (if (nil? buckets)
        (clojure.string/join " " bktsfeatures) ; a list, each item repr a feature string for a bucket.
        (let [bkt (first buckets)
              entries (:entries (bkt facets-data))
              bktvalstats (bucket-vals-stats (name bkt) entries)] ; each bucket gen a feature string
          (recur (next buckets) (conj bktsfeatures bktvalstats)))))))


; read event data, populate a event lookup map keyed with event end-time in long
; {event-endtime {:end end-time :attendance 1000}}
(defn create-event-timetab
  "create event map keyed with event time(long) with event file"
  [evtfile]
  (with-open [rdr (reader evtfile)]
    (loop [evts (line-seq rdr) evtmap {}]
      (if (empty? evts)
        evtmap
        (let [e (first evts)
              fields (clojure.string/split e #",")
              [mdy etime attend] (map (partial nth fields) [0 2 3])
              endtime (parse (formatter "MM/dd/yy HH:mm:ss") (str mdy " " etime))
              endtime-20m (clj-time/minus endtime (clj-time/minutes 20))
              endtime+2h (clj-time/plus endtime (clj-time/hours 2))
              ; evt map keyed with evt time in long
              tbentry (hash-map (to-long endtime-20m) 
                                {:end (to-long endtime+2h) 
                                 :attendance attend})]
          (recur (rest evts) (merge evtmap tbentry)))))))


; format dategeneral namespace features, ret a vw feature row str
;"1375963200000|dategeneral date_year:2013 date_year_2013:1
(defn date-general-feature
  "format namespace dategeneral at time, ret a vw feature row str "
  [timestr]
  (letfn [(tmfeature [[tmvar tmsymb]]  ; pass in var and var symbol pair
            (vector (str "date_" tmsymb ":" tmvar " date_" tmsymb "_" tmvar ":1 ")))]
    (let [tm (parse (formatter "MM/dd/yyyy HH:mm") timestr)
          year (clj-time/year tm)
          month (clj-time/month tm)
          day (clj-time/day tm)
          dayweek (clj-time/day-of-week tm)
          weekyear (int (/ (+ (* 30 month) day) 7))   ; off, but ok
          dayyear (+ (* 30 month) day)
          monthyear month
          hour (clj-time/hour tm)
          minute (clj-time/minute tm)
          featurenames [[year (quote year)]
                        [month (quote month)]
                        [day (quote day)]
                        [dayweek (quote dayweek)]
                        [weekyear (quote weekyear)]
                        [dayyear (quote dayyear)]
                        [monthyear (quote monthyear)]
                        [hour (quote hour)]
                        [minute (quote minute)]]
          feature-str (clojure.string/join (mapcat tmfeature featurenames))]
      ;(prn feature-str)
      (str (to-long tm) "|dategeneral " feature-str))))


; label vw feature row at time MM/dd/yyyy HH:mm as 1 if within game event end time.
(defn label-datapoint
  "label vw feature row at time MM/dd/yyyy HH:mm as 1 if within game event end time"
  [time-str evtmap]
  (let []
    (prn "label data point " time-str)))




              
