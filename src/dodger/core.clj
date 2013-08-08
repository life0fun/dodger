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
    ;(es/delete-index es/dodger-index-name)
    ;(es/create-index-dodger)
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
        time (first args)
        backhours (last args)
        vmfeature (es/gen-feature es/dodger-data-index-name time backhours)]
    (prn "generate feature..." es/dodger-data-index-name time backhours)
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
    "index" (create-index (rest args))
    "gen-feature"  (gen-feature (rest args))
    "train" (train (rest args))
    "predict" (predict (rest args))
    "plot" (plot (rest args))
    (search args)))    ; default
