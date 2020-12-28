(ns tech.v3.dataset.impl.column-base
  (:require [tech.v3.dataset.string-table :as str-table]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.mmap-list :as mmap-list]
            [tech.v3.datatype.packing :as packing])
  (:import java.nio.channels.FileChannel
           java.nio.file.StandardOpenOption
           [java.util List Map]
           tech.v3.dataset.Text
           tech.v3.datatype.PrimitiveList))

(def ^Map dtype->missing-val-map
  {:boolean false
   :int8 Byte/MIN_VALUE
   :int16 Short/MIN_VALUE
   :int32 Integer/MIN_VALUE
   :int64 Long/MIN_VALUE
   :float32 Float/NaN
   :float64 Double/NaN
   :packed-instant (packing/pack (dtype-dt/milliseconds-since-epoch->instant 0))
   :packed-local-date (packing/pack (dtype-dt/milliseconds-since-epoch->local-date 0))
   :packed-duration 0
   :instant nil
   :zoned-date-time nil
   :local-date-time nil
   :local-date nil
   :local-time nil
   :duration nil
   :string ""
   :mmap-string ""
   :text nil
   :keyword nil
   :symbol nil})


(casting/add-object-datatype! :text Text)


(defn datatype->missing-value
  [dtype]
  (let [dtype (if (packing/packed-datatype? dtype)
                dtype
                (casting/un-alias-datatype dtype))]
    (get dtype->missing-val-map dtype
         (when (casting/numeric-type? dtype)
           (casting/cast 0 dtype)))))


(defn make-container
  (^PrimitiveList [dtype n-elems column-options]
   (case dtype
     :string (str-table/make-string-table n-elems "")
     :text (let [^List list-data (dtype/make-container :list :text 0)]
             (dotimes [iter n-elems]
               (.add list-data nil))
             list-data)
     :mmap-string (let [mmap-file
                        (or  (:mmap-file column-options)
                             (java.io.File/createTempFile "tmd" ".mmap"))
                        mmap-file-channel
                        (or (:mmap-file-channel column-options)
                            (FileChannel/open  (.toPath mmap-file)
                                               (into-array [StandardOpenOption/APPEND])))]
                    (mmap-list/->MmapList
                     #(String. %)
                     #(.getBytes %)
                     :mmap-string
                     (.getPath mmap-file)
                     mmap-file-channel
                     (atom [])
                     (atom nil)
                     ))
     (dtype/make-container :list dtype n-elems)))
  (^PrimitiveList [dtype n-elems]
   (make-container dtype n-elems nil))
  (^PrimitiveList [dtype]
   (make-container dtype 0 nil)))
