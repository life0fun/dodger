(ns dodger.core
  (:require [clojure.string :as str])
  (:require [clojure.java.jdbc :as sql])
  (:import [java.io FileReader]
           [java.util Map Map$Entry List ArrayList Collection Iterator HashMap])
  (:require [clj-redis.client :as redis]) ; bring in redis namespace
  (:require [dodger.elastic.es :as es])
  (:require [dodger.incanter.plot :as plot])
  (:require [clj-time.core :as clj-time :exclude [extend]]
            [clj-time.format]
            [clj-time.local])
  (:gen-class :main true))


; populate raw data into index
(defn create-index [args]
  "create index dodger with data, first arg is data file"
  (let [now (clj-time.local/local-now)
        fns (map (fn [nm] (ns-resolve 'clj-time.core (symbol nm))) ["year" "month" "day"])
        datm (map (fn [f] (format "%02d" (f now))) fns)   ; clojure.core/format string
        nowidx (str "logstash-" (clojure.string/join "." datm))
        fmt-now (clj-time.format/unparse (clj-time.format/formatter "yyyy.MM.dd") now)
        nxt-week (clj-time/plus now (clj-time/weeks 1))
        datfile (first args)]
    (prn "create index with data..." datfile)
    ; (es/delete-index es/dodger-data-index-name)
    ; (es/delete-index es/dodger-test-index-name)
    ; (es/create-index-dodger)
    (es/populate-dodger-data-index datfile)))


; generate feature 
(defn gen-feature [args]
  "generate feature based on facet query result"
  (let [now (clj-time.local/local-now)
        fns (map (fn [nm] (ns-resolve 'clj-time.core (symbol nm))) ["year" "month" "day"])
        datm (map (fn [f] (format "%02d" (f now))) fns)   ; clojure.core/format string
        nowidx (str "logstash-" (clojure.string/join "." datm))
        fmt-now (clj-time.format/unparse (clj-time.format/formatter "yyyy.MM.dd") now)
        nxt-week (clj-time/plus now (clj-time/weeks 1))
        timestr (first args) ; gen feature for time data-point
        ;backhours (read-string (last args))
        vmfeature (es/gen-feature es/dodger-data-index-name timestr 24)] ; back 24 hour
    (prn "generate feature..." es/dodger-data-index-name timestr)
    (prn "gen feature : " vmfeature)))


; feed with train data to generate model 
(defn train [args]
  "train vw with the feature data"
  (let [now (clj-time.local/local-now)
        fns (map (fn [nm] (ns-resolve 'clj-time.core (symbol nm))) ["year" "month" "day"])
        datm (map (fn [f] (format "%02d" (f now))) fns)   ; clojure.core/format string
        nowidx (str "logstash-" (clojure.string/join "." datm))
        fmt-now (clj-time.format/unparse (clj-time.format/formatter "yyyy.MM.dd") now)
        nxt-week (clj-time/plus now (clj-time/weeks 1))
        idxname (or args nowidx)
        datfile (first args)
        evtfile (second args)
        mdlfile (last args)]
    (prn "train model with..." datfile evtfile mdlfile)
    (es/train-model datfile evtfile mdlfile)))

; make prediction using model from training 
(defn predict [args]
  "predict traffic with trained model"
  (let [now (clj-time.local/local-now)
        fns (map (fn [nm] (ns-resolve 'clj-time.core (symbol nm))) ["year" "month" "day"])
        datm (map (fn [f] (format "%02d" (f now))) fns)   ; clojure.core/format string
        nowidx (str "logstash-" (clojure.string/join "." datm))
        fmt-now (clj-time.format/unparse (clj-time.format/formatter "yyyy.MM.dd") now)
        nxt-week (clj-time/plus now (clj-time/weeks 1))
        idxname (or args nowidx)
        datfile (first args)
        modelfile (second args)]
    (prn "predict..." datfile)))


(defn search [args]
  (let [now (clj-time.local/local-now)
        fns (map (fn [nm] (ns-resolve 'clj-time.core (symbol nm))) ["year" "month" "day"])
        datm (map (fn [f] (format "%02d" (f now))) fns)   ; clojure.core/format string
        nowidx (str "logstash-" (clojure.string/join "." datm))
        fmt-now (clj-time.format/unparse (clj-time.format/formatter "yyyy.MM.dd") now)
        nxt-week (clj-time/plus now (clj-time/weeks 1))
        idxname (or args nowidx)
        time (first args)]
    (prn "search by..." time)
    (es/query time)))

(defn plot [args]
  ; args ary is ["plot" "/tmp/x"]
  (plot/plot-hs-data (second args)))  ; second arg is log file


; the main 
(defn -main [& args]
  (prn " >>>> elasticsearch to generate feature data for machine learning <<<<< ")
  (case (first args)
    ; lein-2 run index data/train.data
    "index" (create-index (rest args))
    ; lein-2 run gen-feature data/train.data data/Dodgers.events xx
    "gen-feature"  (gen-feature (rest args)) 
    ; lein-2 run train data/train.data data/Dodgers.events output.model
    "train" (train (rest args))
    "predict" (predict (rest args))
    "plot" (plot (rest args))
    (search args)))    ; default
