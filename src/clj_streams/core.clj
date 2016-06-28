(ns clj-streams.core
  (:require [clj-streams.serde :as serde :reload-all true]
            [clojurewerkz.propertied.properties :as p])
  (:import (org.apache.kafka.common.serialization Serde)
           (org.apache.kafka.streams.kstream KStreamBuilder KStream ValueMapper)
           (org.apache.kafka.streams KafkaStreams)
           (java.util Properties)
           (org.apache.kafka.streams.processor TopologyBuilder)))

(defn refine-config
  "refine config and make it ready to be used by kafka streams
  (resolve decorders etc..)"
  [conf
   kser
   vser]
  (-> conf
      (assoc "key.serde" (->> kser
                                   .getClass
                                   .getName))
      (assoc "value.serde" (->> vser
                                     .getClass
                                     .getName)))
      ;(condp = (:KEY_SERDE_CLASS conf)
      ;         :string )
      )


(refine-config {:APPLICATION_ID    "kafka-streams-clojure"
                "bootstrap.servers" "localhost:9092"
                :ZOOKEEPER_CONNECT "localhost:2181"}
               (serde/string)
               (serde/string))


(defn make-streams [conf
                    kser
                    vser
                    in-topic
                    out-topic
                    map-xf]
  (let [ref-conf (refine-config conf kser vser)
        p (p/load-from ref-conf)
        builder (KStreamBuilder.)
        stream (.stream builder
                        kser
                        vser
                        (into-array [in-topic]))
        ]
    (do
      (println p)
      (-> stream
          (.mapValues ^ValueMapper
                      (reify ValueMapper
                        (apply [this value]
                          (map-xf value))))
          (.to out-topic))
      (KafkaStreams. builder p))
    ))

;; hypothetical transformation
(def mapxf (comp #(.toUpperCase %)
                 #(str % "_suffix")))


(with-open [streams
            (make-streams {"application.id"    "kafka-streams-clojure"
                           "bootstrap.servers" "localhost:9092"
                           :ZOOKEEPER_CONNECT "localhost:2181"}
                          (serde/string)
                          (serde/string)
                          "test1"
                          "test2"
                          mapxf)]
  (.start streams)
  (Thread/sleep 10000))