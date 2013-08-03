;;
;; query elastic search engine
;;

(ns logquery.elastic.es
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

(def time-range 1)    ; from now, how far back

; forward declaration
(declare text-query)
(declare stats-query)
(declare format-stats)
(declare trigger-task-query)
(declare process-stats-hits)
(declare process-stats-record)


; wrap connecting fn
(defn connect [host port]
  (esr/connect! (str "http://" host ":" port)))


; eval exprs within a new connection.    
(defmacro with-esconn [[host port] & exprs]
  `(with-open [conn# (esr/connect! (str "http://" ~host ":" ~port))]
     (binding [*es-conn* conn#]                
       (do ~@exprs))))


; document query takes index name, mapping name and query (as a Clojure map)
; curl -s http://127.0.0.1:9200/_status?pretty=true | grep logstash
; curl -XGET 'http://cte-db3:9200/logstash-2013.05.22/_search?q=@type=finder_core_api
(defn test-query-with [tag]
  (with-esconn ["cte-db3" 9200]
    (let [res (esd/search "logstash-2013.05.22" :query (q/term :message "timeInvoked"))
          n    (esrsp/total-hits res)
          hits (esrsp/hits-from res)]
      (println (format "Total hits: %d" n))
      (pp/pprint hits))))


(defn test-query [idxname query process-fn]
  ; if idxname is unknown, we can use search-all-indexes-and-types.
  ; query range to be 
  (connect "cte-db3" 9200)
  ;(connect "localhost" 9200)           
  (let [res (esd/search-all-indexes-and-types ;esd/search-all-types idxname ;"logstash-2013.05.22"
              :size 100
              :query query
              :sort {"@timestamp" {"order" "desc"}})
         n (esrsp/total-hits res)
         hits (esrsp/hits-from res)
         f (esrsp/facets-from res)]
    (println (format "Total hits: %d" n))
    (process-fn hits)))


(defn test-stats-query [idxname]
  (test-query idxname (stats-query) process-stats-hits))


(defn test-trigger-query [idxname]
  ;(test-query idxname (trigger-task-query) prn))
  (test-query idxname (text-query) prn))


(defn text-query []
  (q/text "text" "elapsed"))


(defn stats-query []
  ; form query params for Stats query. time range from yesterday to today.
  ; logstash column/field begin with @
  (let [now (clj-time/now) 
        pre (clj-time/minus now (clj-time/days time-range))  ; from now back 2 days
        nowfmt (clj-time.format/unparse (clj-time.format/formatters :date-time) now)
        prefmt (clj-time.format/unparse (clj-time.format/formatters :date-time) pre)]
    (q/filtered
      :query 
        (q/query-string 
          :default_field "@message"
          :query "@message:Stats AND @type:finder_core_application")
      :filter 
        {:range {"@timestamp" {:from prefmt     ;"2013-05-29T00:44:42"
                               :to nowfmt }}})))


(defn trigger-task-query []
  ; form query params to search elapsed keyword in log message.
  (q/filtered :query
    (q/query-string
      :default_field "@message"
      :query "@message:elapsed AND @type:finder_core_accounting_triggeredTask")))


; hits contains log message
(defn process-stats-hits [hits]
  ; query result contains hits map, extract log message from hits map
  (let [msgs (map (fn [row] (-> row :_source (get (keyword "@message")))) hits)
        stats (map process-stats-record msgs)
        test-result (map format-stats stats)] ; for each msg record, extract timestamp and stats field
    (view-stats-data test-result)))    ; map format stat fn to each stats in each record


(defn process-stats-record [record]
  ; for each log record, ret the first and 5th fields (timestamp and stats)
  (let [fields (clojure.string/split record #"\|")]  ; log delimiter is |
    ;(prn fields (count fields))
    (map #(nth fields %) [1 5])))

(defn format-stats [stats] 
  ; stats is ("Fri May 24.." "Stats = CNI{hs=10, time=20}")
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
    