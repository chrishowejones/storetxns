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

(defn mk-fixed-batch-spout [max-batch-size]
  (FixedBatchSpout.
   ;; name the tuples the spout will emit
   (t/fields "txnmessage")
   max-batch-size
   (into-array (map t/values ["{\"accnum\":123456, \"balance\": 100.00, \"amount\":10.00, \"txn-type\":\"debit\"}"]))))

(def ^:private txnmessage "txnmessage")

(def ^:private txn-scheme
  (reify Scheme
    (deserialize [this bytes]
      (->
       (String. bytes "UTF-8")
       (t/values)))
    (getOutputFields [this]
      (t/fields txnmessage))))

(def ^:private txn-message-scheme
  (SchemeAsMultiScheme. txn-scheme))

(defn kafka-spout [{:keys [kafka topic]} spout-name]
  (let [{:keys [zookeeper]} kafka
        zk-hosts (ZkHosts. zookeeper)
        config (TridentKafkaConfig. zk-hosts topic spout-name)]
    (set! (. config scheme) txn-message-scheme)
    (OpaqueTridentKafkaSpout. config)))

(t/deftridentfn txn-msg->tuple
  [tuple coll]
  (when-let [message (t/get tuple txnmessage)]
    (try
     (let [{:keys [accnum balance amount txn-type]} (parse-string message true)]
       (t/emit-fn coll accnum balance amount txn-type))
     (catch com.fasterxml.jackson.core.JsonParseException ex
         (t/emit-fn coll nil nil nil nil)))))

(defn build-topology [spout]
  (let [trident-topology (TridentTopology.)]
    (-> (t/new-stream trident-topology "storeTxns" spout)
        (t/each [txnmessage] txn-msg->tuple ["accnum" "balance" "amount" "txn-type"])
        (t/each ["accnum"] (filter-null))
        (t/debug))
    (.build trident-topology)))
