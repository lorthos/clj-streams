(ns clj-streams.serde
  (:import (org.apache.kafka.common.serialization Serdes)))

(defn string []
  (Serdes/String))


(def all
  {:string (fn [] (string))})

