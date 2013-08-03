;
; query with facet aggregations to perform statistics
;

(ns logquery.elastic.facet
  (:require [clojure.string :as str])
  (:require [clojure.java.jdbc :as sql])
  (:import [java.io FileReader]
           [java.util Map Map$Entry List ArrayList Collection Iterator HashMap])
  (:require [clj-redis.client :as redis])    ; bring in redis namespace
  (:require [clojure.data.json :as json])
  (:require [clojurewerkz.elastisch.rest          :as esr]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clojurewerkz.elastisch.query         :as q]
            [clojurewerkz.elastisch.rest.response :as esrsp]
            [clojure.pprint :as pp])
  (:require [clj-time.core :as clj-time :exclude [extend]]
            [clj-time.format])
  (:require [logquery.incanter.plot :refer :all]))
;
; http://www.slideshare.net/clintongormley/terms-of-endearment-the-elasticsearch-query-dsl-explained
; curl -XGET 'http://cte-db3:9200/logstash-2013.05.22/_search?q=@type=finder_core_api
; kibana curls to elastic on browser tools, jsconsole, network, then issue a search, check kibana section.
; "curl -XGET http://localhost:9200/logstash-2013.05.23/_search?pretty -d ' 
; {"size": 100, 
;  "query": {
;     "filtered":{
;       "query":{ 
;         "query_string":{
;           "default_operator": "OR", default_field": "@message",
;           "query": "@message:Stats AND @type:finder_core_application"}}, <- close of query
;       "filter": {
;         "range": {
;           "@timestamp": {
;             "from": "2013-05-22T16:10:48Z", "to": "2013-05-23T02:10:48Z"}}}}},
;   "from": 0,
;   "sort": {
;     "@timestamp":{
;       "order": "desc"}},

; For elasticsearch time range query. You can use DateTimeFormatterBuilder, or
; always use (formatters :data-time) formatter.
;

; globals
(def ^:dynamic *es-conn*)

(def time-range 1)    ; from now, just 1 day back

; forward declaration
(declare query-string-keyword)
(declare process-record)
(declare text-query-filter)
(declare elastic-query)

(declare query-stats)
(declare process-stats-hits)
(declare format-stats)

(declare query-email)
(declare process-email-hits)
(declare format-email)


(def elasticserver "cte-db3")
(def elasticport 9200)

; wrap connecting fn
(defn connect [host port]
  (esr/connect! (str "http://" host ":" port)))


; @Deprecated! this one does not work.
(defmacro with-esconn [[host port] & exprs]
  `(with-open [conn# (esr/connect! (str "http://" ~host ":" ~port))]
     (binding [*es-conn* conn#]                
       (do ~@exprs))))


(defn query-string-keyword [keyword]
  "format query params with time range from yesterday to today.
   logstash column/field begin with @, eg :@type, :@message, :@tag "
  (let [now (clj-time/now) 
        ;pre (clj-time/minus now (clj-time/days time-range))  ; from now back 2 days
        pre (clj-time/minus now (clj-time/hours 20))  ; from now back 1 days
        nowfmt (clj-time.format/unparse (clj-time.format/formatters :date-time) now)
        prefmt (clj-time.format/unparse (clj-time.format/formatters :date-time) pre)]
    ; use filtered query
    (q/filtered
      :query
        (q/query-string 
          :default_field "@message"
          ;:query "@message:EmailAlertDigestEventHandlerStats AND @type:finder_core_application")
          :query (str "@message:" keyword))
      :filter 
        {:range {"@timestamp" {:from prefmt   ;"2013-05-29T00:44:42"
                               :to nowfmt }}})))


; ret a map that specifies date histogram query
(defn date-hist-facet [name keyfield valfield interval]
  "form a date histogram facets map, like {name : {date_histogram : {field: f}}}"
  (let [qmap (hash-map "field" keyfield "interval" interval)]
    (if (nil? valfield)
      (hash-map name (hash-map "date_histogram" qmap))
      (hash-map name (hash-map "date_histogram" (assoc qmap "value_field" valfield))))))


; output :facets {:facet-name {:_type "terms" :terms [{:term "vci" :count 12} {:term "finder" :count 45}] }}
(defn term-facet [name keyfield]
  "form a term facets map, like {name : {terms : {field: f}}}"
  (let [tmap (hash-map "terms" (hash-map "field" keyfield))]
    (hash-map name tmap)))


(defn elastic-query [idxname keyword process-fn]
  ; if idxname is unknown, we can use search-all-indexes-and-types.
  ; query range to be 
  ;(with-esconn [elasticserver elasticport]
  (connect elasticserver elasticport)
  (let [qrystr (query-string-keyword keyword)
        res (esd/search-all-types idxname ;esd/search-all-types idxname ;"logstash-2013.05.22"
              :size 2
              :query qrystr
              ;:facets (term-facet "logtags" "@tags")
              :facets (date-hist-facet "chart0" "@timestamp" nil "10m")
              :sort {"@timestamp" {"order" "desc"}})
        n (esrsp/total-hits res)
        hits (esrsp/hits-from res)
        fres (esrsp/facets-from res)]
    (println (format "Total hits: %d" n))
    (process-fn fres)
    (process-fn hits))
    res)   ; ret response


(defn test-date-hist [idxname]
  (prn "test-date-hist : " idxname)
  (elastic-query idxname "consumer" pp/pprint))



(defn test-trigger-query [idxname]
  ;(test-query idxname (trigger-task-filter) prn))
  (elastic-query idxname (text-query-filter) prn))


(defn trigger-task-filter []
  ; form query params to search elapsed keyword in log message.
  (q/filtered :query
    (q/query-string
      :default_field "@message"
      :query "@message:elapsed AND @type:finder_core_accounting_triggeredTask")))


(defn text-query-filter []
  ; filter to query text field with elapsed keyword
  (q/text "text" "elapsed"))


(defn process-record [record]
  ; logstash log has the following format, [type | timestamp | file-name | log-content ]
  ; for each log record, ret the first and 5th fields (timestamp and stats)
  (let [fields (clojure.string/split record #"\|")]  ; log delimiter is |
    ;(prn fields (count fields))
    (map #(nth fields %) [1 5])))


(defn query-stats [idxname]
  (elastic-query idxname 
                 (query-string-keyword "CNIContactUsageEventHandlerStats") 
                 process-stats-hits))


; hits contains log message
(defn process-stats-hits [hits]
  ; query result contains hits map, extract log message from hits map
  (let [msgs (map (fn [row] (-> row :_source (get (keyword "@message")))) hits)
        stats (map process-record msgs)
        test-result (map format-stats stats)] ; for each msg record, extract timestamp and stats field
    ;(prn test-result)))    ; testing
    (view-stats-data test-result)))  ;map format stat fn to each stats in each record


(defn format-stats [stats] 
  ; stats is ("Fri May 24.." "Stats = CNI{hs=10, time=20}")
  ;(prn stats))
  ; convert key=val to kv map using regexp. first, capture inside {}, match key=val.
  (let [ts (clojure.string/join " " (reverse (map #(first (clojure.string/split % #"\.")) (take 3 (rest (clojure.string/split (clojure.string/trim (first stats)) #"\s+"))))))
        ;tsf (clj-time.format/unparse (clj-time.format/formatters :date-hour-minute-second) (clj-time.format/parse-local ts))
        st (first (re-find #"\{(.*?)\}" (last stats)))  ; capture non-greedy everything inside {}
        kv (re-seq #"([^=}{]+)=([^=}{]+)(?:,|$|}|\s+)" st) ; capture (any except ={) = () as kv pair seq
        ; after reg capture, each entry has 3 ele, [k=v, k, v], extract k v
        statmap (zipmap (map #(% 1) kv) (map #(-> % (nth 2) (->> (re-find #"\d+"))) kv))
        elapsed (quot (read-string (re-find #"\d+" (get statmap "timeTaken"))) 64000)]    ; 64 threads
    (prn "elapse time: " elapsed "successEvents:" (get statmap "successfulEventCount") (rest stats))
    (println)
    (assoc statmap :elapse elapsed :timestamp ts)))  ; ret the map
  

;; section for handling email query
(defn query-email [idxname]
  (elastic-query idxname 
                 (query-string-keyword "EmailAlertDigestEventHandlerStats")
                 process-email-hits))


; hits contains log message
(defn process-email-hits [hits]
  ; query result contains hits map, extract log message from hits map
  (let [msgs (map (fn [row] (-> row :_source (get (keyword "@message")))) hits)
        emailstats (map process-record msgs)
        test-result (map format-email emailstats)] ; for each msg record, extract timestamp and stats field
    ;(prn (first test-result))))
    (view-email-data test-result)))  ;map format stat fn to each stats in each record
                

(defn format-email [stats] 
  ; stats is ("Fri May 24.." "Stats = CNI{hs=10, time=20}")
  ;(prn stats))
  ; convert key=val to kv map using regexp. first, capture inside {}, match key=val.
  (let [ts (clojure.string/join " " (reverse (map #(first (clojure.string/split % #"\.")) (take 3 (rest (clojure.string/split (clojure.string/trim (first stats)) #"\s+"))))))
        ;tsf (clj-time.format/unparse (clj-time.format/formatters :date-hour-minute-second) (clj-time.format/parse-local ts))
        st (first (re-find #"\{(.*?)\}" (last stats)))  ; capture non-greedy everything inside {}
        kv (re-seq #"([^=}{]+)=([^=}{]+)(?:,|$|}|\s+)" st) ; capture (any except ={) = () as kv pair seq
        ; after reg capture, each entry has 3 ele, [k=v, k, v], extract k v
        statmap (zipmap (map #(% 1) kv) (map #(-> % (nth 2) (->> (re-find #"\d+"))) kv))
        elapsed (quot (read-string (re-find #"\d+" (get statmap "totalTimeTaken"))) 64000)]    ; 64 threads
    (prn "elapse time: " elapsed "numberOfEmailsSent:" (get statmap "numberOfEmailsSent") statmap)
    (println)
    (assoc statmap :elapse elapsed :timestamp ts)))  ; ret the map