(ns tech.v3.libs.jarrow
  (:require [clojure.java.io :as io]
            [tech.v3.datatype :as dtype]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.io :as ds-io])

  (:import [uk.ac.bristol.star.feather FeatherTable]))



(defn make-reader [feather-reader feather-type row-count]
  (case feather-type
    "DOUBLE" (dtype/make-reader :float64 row-count (.getDouble feather-reader idx))
    "INT32" (dtype/make-reader :int32 row-count (.getInt feather-reader idx))
    "UTF8" (dtype/make-reader :string row-count (.getObject feather-reader idx))))
    ;; TODO add more types




(defn add-feather-col [dataset idx feather-table]
  (let [
        column (.getColumn feather-table idx)

        feather-reader
        (.createReader column)

        feather-type
        (-> (.. column getDecoder getFeatherType)
            str)

        row-count
        (.getRowCount feather-table)

        col-name (.getName column)

        reader
        (make-reader feather-reader feather-type row-count)]

    (-> dataset
        (ds/new-column col-name reader))))


(defn- feather->dataset [fpath]
  (let [feather-table
        (FeatherTable/fromFile (io/file fpath))]
    (println :version (.getFeatherVersion feather-table))
    (reduce (fn [dataset col-idx]
              (add-feather-col dataset col-idx feather-table))
            (ds/empty-dataset)
            (range (.getColumnCount feather-table)))))

(defmethod ds-io/data->dataset :feather
  [_data options]
  (feather->dataset _data))





;; arrow::write_feather(iris,"/tmp/iris.feather_arrow")
;; feather::write_feather(iris,"/tmp/iris.feather_feather")
;; arrow::write_ipc_stream(iris,"/tmp/iris.ipc_stream")
;; arrow::write_arrow(iris,"/tmp/iris.arrow") ; same as arrow::write_feather

(comment
  ;;  3 ways to write from R
  ;; arrow::write_feather(iris,"/tmp/iris.feather_arrow")
  ;; feather::write_feather(iris,"/tmp/iris.feather_feather")
  ;; arrow::write_ipc_stream(iris,"/tmp/iris.ipc_stream")
  ;; arrow::write_arrow(iris,"/tmp/iris.arrow") ; same as arrow::write_feather


  ;; first

  (arrow/stream->dataset "/tmp/iris.feather_arrow")
       ;;  fails
     ;; No method in multimethod 'parse-message' for dispatch value:
     ;; {:unexpected-message-type 0}


  (feather->dataset              ;; using jarrow
   "/tmp/iris.feather_feather")   ;> ok

  (arrow/stream->dataset "/tmp/iris.ipc_stream") ; > ok

  (arrow/stream->dataset "/tmp/iris.arrow")
  ;;  fails
  ;; No method in multimethod 'parse-message' for dispatch value:
  ;; {:unexpected-message-type 0}



  :ok)

(comment
  (require '[tech.v3.libs.arrow :as arrow])
  (arrow/stream->dataset "/tmp/iris.feather_feather"))
