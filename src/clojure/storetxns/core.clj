(ns storetxns.core
  (:gen-class)
  (:require [clojure.string :as str]
            [storetxns.persist-txns :refer [mk-fixed-batch-spout kafka-spout build-topology]])
  (:import [backtype.storm LocalCluster StormSubmitter]))

(def config
  {:topic "devcycle123"
   :kafka {:zookeeper "localhost:2181"}})

(def remote-config
  {:topic "devcycle123"
   :kafka {:zookeeper "hostgroupmaster1-3-lloyds-20150923072909"}})

(defn run-local! []
  (let [cluster (LocalCluster.)
        spout (kafka-spout config "txnspout")]
    (.submitTopology cluster "storetxns"
                     {}
                     (build-topology spout)))
  (Thread/sleep 5000)
  "completed")

(defn submit-topology! []
  (let [name "storetxns"
        conf {}
        spout (kafka-spout remote-config "txnspout")]
    (StormSubmitter/submitTopology
     name
     conf
     (build-topology
      spout))))

(def ^:private app-specs [["-h" "--help" "Print this help"]
                ["-r" "--remote" "Submit the topology to a remote cluster"]])

(defn- usage [options-summary]
  (->> ["This topology reads transaction messages from a Kafka topic."
        ""
        "Usage: storm jar storetxns.jar [options]"
        ""
        "Options:"
        options-summary]
       (str/join \newline)))

(defn output-help [summary]
  (println (usage summary)))

(defn -main
  "Run the topology in local or remote mode - defaults to local"
  [& args]
  (if (= "remote" (first args))
    (submit-topology!)
    (run-local!)))


(comment
  ;; kafka spout
  (kafka-spout config "txnspout")

  ;; fixed batch spout
  (doto (mk-fixed-batch-spout 3)
                (.setCycle true))
  )
