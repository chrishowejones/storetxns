(defproject storetxns "0.1.0-SNAPSHOT"
  :description "Example Storm topology to read txn messages from Kafka and persist to HBase"
  :url "http://"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [yieldbot/marceline "0.2.1"]
                 [cheshire "5.5.0"]
                 [org.apache.storm/storm-kafka "0.10.0.2.3.0.0-2557"
                  :exclusions [org.slf4j/slf4j-log4j12]]
                 [org.apache.kafka/kafka_2.10 "0.8.2.2.3.0.0-2557"
                  :exclusions [org.slf4j/slf4j-log4j12]]
                 [log4j/log4j "1.2.17"]]
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :main storetxns.core
  :target-path "target/%s"
  :profiles {:dev {:aot :all}
             :provided {:dependencies [[org.apache.storm/storm-core "0.10.0.2.3.0.0-2557"]]}
             :uberjar {:aot :all}}
  :repositories [["HDPReleases" "http://nexus-private.hortonworks.com/nexus/content/groups/public/"]])
