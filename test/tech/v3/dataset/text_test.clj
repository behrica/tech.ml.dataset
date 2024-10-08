(ns tech.v3.dataset.text-test
  (:require [tech.v3.dataset :as ds]))

(->>
 (ds/->dataset "test/data/medical-text.csv" {:text-temp-dir "/tmp/text-temp"})
 (ds/columns)
 (map meta))