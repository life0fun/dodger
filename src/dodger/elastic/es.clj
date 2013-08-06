(ns dodger.elastic.es
  (:require [clojure.string :as str])
  (:require [clojure.java.jdbc :as sql])
  (:import [java.io FileReader]
           [java.util Map Map$Entry List ArrayList Collection Iterator HashMap])
  (:require [clj-redis.client :as redis])    ; bring in redis namespace
  (:require [clojure.data.json :as json]
            [clojure.java.io :only [reader] :refer [reader]])
  (:require [clojurewerkz.elastisch.rest          :as esr]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clojurewerkz.elastisch.rest.index    :as esi]
            [clojurewerkz.elastisch.query         :as q]
            [clojurewerkz.elastisch.rest.response :as esrsp]
            [clojure.pprint :as pp])
  (:require [clj-time.core :as clj-time :exclude [extend]]
            [clj-time.format :refer [parse unparse formatter]]
            [clj-time.coerce :refer [to-long from-long]])
  (:require [dodger.incanter.plot :refer :all]
            [dodger.vwfeature :refer :all]))
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

(def elasticserver "localhost")
(def elasticport 9200)

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


; mapping types can be thought of as tables in a db(index)
; mapping types defines how a field is analyzed, indexed so can be searched.
(defn create-dodger-mapping-types 
  "ret a mapping type for dodger data with timestamp and value fields"
  [mapping-name]
  (let [name mapping-name
        schema (hash-map :value {:type "integer" :store "yes"}
                         :timestamp {:type "date" :format "MM/dd/yyyy HH:mm"})]
    (hash-map name (hash-map :properties schema))))

(defn create-dodger-doc
  "create a document for dodger mapping, :value and :timestamp fields"
  [value timestamp]
  (hash-map :value value :timestamp timestamp))

; document query takes index name, mapping name and query (as a Clojure map)
; curl -s http://127.0.0.1:9200/_status?pretty=true | grep logstash
(defn create-index
  "create index with mappings"
  [idxname mappings]
  (if (esi/exists? idxname)
    (esi/delete idxname)
    ;(prn (esi/get-settings idxname))
    (esi/create idxname :mappings mappings)))


(defn populate-dodger-data
  "read data file line by line, and create document in es with fields"
  [idxname mappings datfile]
  ; transform file as line seq using clojure.java.io/reader
  (prn "populating from datfile " datfile)
  (with-open [rdr (reader datfile)]
    (doseq [l (line-seq rdr)]
      (let [[ts val] (clojure.string/split l #",")]
        (prn ts val)
        (esd/create idxname mappings (create-dodger-doc val ts))
        val))))


(defn create-index-with-data 
  "create dodger index with data file"
  [idxname datfile]
  (let [mapping-name "timevalue"
        mappings (create-dodger-mapping-types mapping-name)]
    (create-index idxname mappings)
    (populate-dodger-data idxname mapping-name datfile)
    idxname))


(defn query-string-keyword [field keyword]
  "ret a query map with filtered clause"
  (let [now (clj-time/now) 
        ;pre (clj-time/minus now (clj-time/days time-range))  ; from now back 2 days
        pre (clj-time/minus now (clj-time/hours 20))  ; from now back 1 days
        nowfmt (clj-time.format/unparse (clj-time.format/formatters :date-time) now)
        prefmt (clj-time.format/unparse (clj-time.format/formatters :date-time) pre)]
    ; use filtered query
    (q/filtered
      :query
        (q/query-string 
          :default_field field
          ;:query "@message:EmailAlertDigestEventHandlerStats AND @type:finder_core_application")
          :query (str "value" keyword))
      :filter 
        {:range {"timestamp" {:from prefmt   ;"2013-05-29T00:44:42"
                               :to nowfmt }}})))


; ret a map that specifies date histogram query, specify key field and value_field.
(defn date-hist-facet [name keyfield valfield interval]
  "ret a date histogram facets map, like {name : {date_histogram : {field: f}}}"
  (let [name (str name "-" interval)
        qmap (hash-map "field" keyfield "interval" interval)]
    (if (nil? valfield)
      (hash-map name (hash-map "date_histogram" qmap))
      (hash-map name (hash-map "date_histogram" (assoc qmap "value_field" valfield))))))


; construct filtered query for time ranged query
(defn filtered-time-range
  "filtered query with filter range on timestamp field from cur point back to n hours"
  [field timestr backhours] ; all args in string format
  (let [to (parse (formatter "MM/dd/yyyy HH:mm") timestr)
        from (clj-time/minus to (clj-time/hours backhours))
        from-str (unparse (formatter "MM/dd/yyyy HH:mm") from)]
    ;(q/range field :from from-str :to timestr)))
    (q/range field :from (to-long from) :to (to-long to))))


(defn query 
  "query using the passed in query clause"
  ([time]
    (query "dodger" "*" (date-hist-facet "datehist" "timestamp" "value" "10m") pp/pprint))

  ([idxname query-clause facet-clause process-fn]
    (connect elasticserver elasticport)
    (let [res (esd/search-all-types idxname ;esd/search-all-types idxname ;"logstash-2013.05.22"
                :size 2
                :query query-clause
                :facets facet-clause
                :sort {"timestamp" {"order" "desc"}})
          n (esrsp/total-hits res)
          hits (esrsp/hits-from res)
          fres (esrsp/facets-from res)]
      (prn (format "Total hits: %d" n))
      (process-fn hits)
      (process-fn (keys fres))
      res)))   ; ret response


; search with a list of facets
(defn last-day-facets
  "generate vm feature using a list of date histogram facet query starting from time"
  [idxname timestr backhours] 
  (let [tm (parse (formatter "MM/dd/yyyy HH:mm") timestr)
        query-clause (filtered-time-range "timestamp" timestr backhours)
        intervals ["5m" "10m" "30m" "1h"]  ; 4 buckets per hour
        facet-ary (map (partial date-hist-facet (str "time-" backhours "h-bucket") "timestamp" "value") intervals)
        facet-map (reduce merge facet-ary)]  ; merge all facets query map
    (prn "vm-facets : " (keys facet-map))
    (query idxname query-clause facet-map pp/pprint)))
  
(defn last-week-facets
  "generate vm feature using a list of date histogram facet query starting from time"
  [idxname timestr backdays] 
  (let [tm (parse (formatter "MM/dd/yyyy HH:mm") timestr)
        backhours (* 24 backdays)
        query-clause (filtered-time-range "timestamp" timestr backhours)
        intervals ["1h" "2h" "6h" "12h" "24h"]  ; 4 buckets per hour
        facet-ary (map (partial date-hist-facet (str "time-" backdays "d-bucket") "timestamp" "value") intervals)
        facet-map (reduce merge facet-ary)]  ; merge all facets query map
    (prn "vm-facets : " (keys facet-map))
    (query idxname query-clause facet-map pp/pprint)))

(defn gen-feature
  "generate a feature row data based on facet query for a particular time in event"
  [idxname timestr backhours]
  (let [row (str " |")
        backhours (read-string backhours)
        query-last-day-clause (filtered-time-range "timestamp" timestr backhours)
        res (last-day-facets idxname timestr backhours)
        feature-last-day (vw-feature-data-format (esrsp/facets-from res))
        query-last-week-clause (filtered-time-range "timestamp" timestr (* 24 7))
        res (last-week-facets idxname timestr 7)
        feature-last-week (vw-feature-data-format (esrsp/facets-from res))
        ]
    ;(prn "gen-feature for event at time " idxname timestr backhours)
    ;(query idxname query-clause pp/pprint)))
    ;(prn "feature data :" feature-last-week)
    (-> row 
        (str feature-last-day)
        (str " | " feature-last-week))))




