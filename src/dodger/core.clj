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


; create index
(defn create-index [args]
  (let [now (clj-time.local/local-now)
        fns (map (fn [nm] (ns-resolve 'clj-time.core (symbol nm))) ["year" "month" "day"])
        datm (map (fn [f] (format "%02d" (f now))) fns)   ; clojure.core/format string
        nowidx (str "logstash-" (clojure.string/join "." datm))
        fmt-now (clj-time.format/unparse (clj-time.format/formatter "yyyy.MM.dd") now)
        nxt-week (clj-time/plus now (clj-time/weeks 1))
        idxname (or args nowidx)
        datfile (first args)]
    (prn "create-index..." idxname fmt-now nowidx nxt-week)
    (es/create-index idxname datfile)))


; generate feature 
(defn gen-feature [args]
  (let [now (clj-time.local/local-now)
        fns (map (fn [nm] (ns-resolve 'clj-time.core (symbol nm))) ["year" "month" "day"])
        datm (map (fn [f] (format "%02d" (f now))) fns)   ; clojure.core/format string
        nowidx (str "logstash-" (clojure.string/join "." datm))
        fmt-now (clj-time.format/unparse (clj-time.format/formatter "yyyy.MM.dd") now)
        nxt-week (clj-time/plus now (clj-time/weeks 1))
        idxname (or args nowidx)
        datfile (first args)
        time (second args)]
    (prn "generate feature..." idxname time)
    (es/gen-feature idxname time)))


; generate feature 
(defn train [args]
  (let [now (clj-time.local/local-now)
        fns (map (fn [nm] (ns-resolve 'clj-time.core (symbol nm))) ["year" "month" "day"])
        datm (map (fn [f] (format "%02d" (f now))) fns)   ; clojure.core/format string
        nowidx (str "logstash-" (clojure.string/join "." datm))
        fmt-now (clj-time.format/unparse (clj-time.format/formatter "yyyy.MM.dd") now)
        nxt-week (clj-time/plus now (clj-time/weeks 1))
        idxname (or args nowidx)
        datfile (first args)
        modelfile (second args)]
    (prn "train..." datfile modelfile)
    (es/train datfile modelfile)))

; generate feature 
(defn predict [args]
  (let [now (clj-time.local/local-now)
        fns (map (fn [nm] (ns-resolve 'clj-time.core (symbol nm))) ["year" "month" "day"])
        datm (map (fn [f] (format "%02d" (f now))) fns)   ; clojure.core/format string
        nowidx (str "logstash-" (clojure.string/join "." datm))
        fmt-now (clj-time.format/unparse (clj-time.format/formatter "yyyy.MM.dd") now)
        nxt-week (clj-time/plus now (clj-time/weeks 1))
        idxname (or args nowidx)
        datfile (first args)
        modelfile (second args)]
    (prn "predict..." datfile)
    (es/predict datfile)))


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
    (es/search time)))

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
    (search-data args)))    ; default
