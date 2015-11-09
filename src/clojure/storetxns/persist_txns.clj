(ns storetxns.persist-txns
  (:require [marceline.storm.trident :as t]
            [marceline.storm.builtin :refer [filter-null]]
            [cheshire.core :refer [parse-string]])
  (:import [storm.trident TridentTopology]
           [storm.kafka ZkHosts StringScheme]
           [storm.kafka.trident OpaqueTridentKafkaSpout
            TridentKafkaConfig]
           [storm.trident.testing
            FixedBatchSpout
            MemoryMapState$Factory]
           [backtype.storm.spout Scheme SchemeAsMultiScheme]))

(defn mk-fixed-batch-spout
  "Used for testing - just cycles producing same message repeatedly"
  [max-batch-size]
  (FixedBatchSpout.
   ;; name the tuples the spout will emit
   (t/fields "txnmessage")
   max-batch-size
   (into-array (map t/values ["{\"accnum\":123456, \"txn-number\":1, \"balance\": 100.00, \"amount\":10.00, \"txn-type\":\"debit\"}"]))))

;;
;; Define strings to for field names and parsed fields in JSON.
;;
(def ^:private txnmessage "txnmessage")

(def ^:private txn-number "txn-number")

(def ^:private accnum "accnum")

(def ^:private balance "balance")

(def ^:private amount "amount")

(def ^:private txn-type "txn-type")

;; Scheme to map message
(def ^:private txn-scheme
  (reify Scheme
    (deserialize [this bytes]
      (->
       (String. bytes "UTF-8")
       (t/values)))
    (getOutputFields [this]
      (t/fields txnmessage))))
;; Use txn-scheme defined above
(def ^:private txn-message-scheme
  (SchemeAsMultiScheme. txn-scheme))

(defn kafka-spout
  "Takes two arguments: a map of the kafka config map and the topic, plus the spout-nane.\n
  returns a Kafka Spout configured appropriately."
  [{:keys [kafka topic]} spout-name]
  (let [{:keys [zookeeper]} kafka
        zk-hosts (ZkHosts. zookeeper)
        config (TridentKafkaConfig. zk-hosts topic spout-name)]
    (set! (. config scheme) txn-message-scheme)
    (OpaqueTridentKafkaSpout. config)))

(t/deftridentfn txn-msg->tuple
  [tuple coll]
  (when-let [message (t/get tuple txnmessage)]
    (try
     (let [{:keys [txn-number, accnum balance amount txn-type]} (parse-string message true)]
       (t/emit-fn coll txn-number accnum balance amount txn-type))
     (catch com.fasterxml.jackson.core.JsonParseException ex
         (t/emit-fn coll nil nil nil nil nil)))))

(defn build-topology
  "Build a topology using Trident that consumes transaction messages from Kafka, and stores a row in HBase per account per day with
 each transaction in a separate column per txn per field in a row."
  [spout]
  (let [trident-topology (TridentTopology.)]
    (-> (t/new-stream trident-topology "storeTxns" spout)
        (t/each [txnmessage] txn-msg->tuple [txn-number accnum balance amount txn-type])
        (t/each [txn-number accnum] (filter-null))
        (t/debug))
    (.build trident-topology)))
