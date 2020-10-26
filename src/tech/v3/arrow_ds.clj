(ns tech.v3.arrow-dsi
  (:require [tech.v3.protocols.dataset :as ds-proto]
            [tech.v3.dataset :as ds]

            [tech.v3.dataset.column :as column]
            [tech.v3.libs.arrow :as arrow]
            [tech.v3.datatype :as dtype]
            [tech.v3.dataset.readers :as ds-readers]
            [tech.v3.datatype.protocols :as dtype-proto]
            [clojure.java.io :as io]
            [tech.v3.datatype.argops :as argops]

            )
  (:import [clojure.lang Counted])
  )

;; (deftype ArrowDS [fname]
;;   ds-proto/PColumnarDataset
;;   (columns [dataset]
;;     (->
;;      (arrow/read-stream-dataset-inplace fname)
;;      (ds/columns)
;;      )
;;     )
;;   (select [dataset column-name-seq-or-map index-seq]
;;     (->
;;      (arrow/read-stream-dataset-inplace fname)
;;      (ds/select column-name-seq-or-map index-seq)
;;      ))
;;   Counted
;;   (count [this]
;;     (->
;;      (arrow/read-stream-dataset-inplace fname)
;;      (ds/row-count)
;;      )
;;     )
;;   )

(defn make-indexes [fnames]
  (let
      [arrows (map arrow/read-stream-dataset-inplace fnames)
       counts (map ds/row-count arrows)
       cumsum-counts (reductions + (conj counts 0) )
       file-ranges
       (->>
        cumsum-counts
        (partition 2 1)
        (map-indexed (fn [index [start end] ]
                       {:range (range start end)
                        :index (repeat   (- end start )
                                         index)
                        }))
        ((fn [ranges]
           {
            :file-index (flatten (map :index ranges))
            :global-index (flatten (map :range ranges))}
           )))
       offsets
       (zipmap (range)
               (drop-last cumsum-counts))

       global->file-index-map
       (zipmap (:global-index file-ranges )
               (:file-index file-ranges)
               )
       ]
      (hash-map
       :offsets offsets
       :global->file-index-map global->file-index-map
       ))
  )


(deftype DirectoryDS [fnames indexes]
  java.util.Map
  (size [this]    (.count this))
  (isEmpty [this] (not (pos? (.count this))))
  (containsValue [this v] (throw (UnsupportedOperationException.)))
  (get [this k] (->> (map #(arrow/read-stream-dataset-inplace %) fnames)
                     (map #(ds/column % k))
                     ((fn [columns]
                        (column/new-column
                         (column/column-name (first columns))
                         (reduce concat columns)
                         (get  (first columns) "metadata")

                         )))

                     ;; (map dtype/->reader)
                     ;; (reduce concat)
                     ))
  (put [this k v]  (throw (UnsupportedOperationException.)))
  (remove [this k] (throw (UnsupportedOperationException.)))
  (putAll [this m] (throw (UnsupportedOperationException.)))
  (clear [this]    (throw (UnsupportedOperationException.)))
  ds-proto/PColumnarDataset
  (columns [this]
    (->>
     (map
      #(arrow/read-stream-dataset-inplace %)
      fnames)
     (apply ds/concat)
     (ds/columns)))
  (select [dataset column-name-seq-or-map index-seq]
    (println "column-seq" column-name-seq-or-map)
    (println "index-seq" (take-last 10 index-seq))

    (->> index-seq
         (map
          #(let [file-index
                 (get (indexes :global->file-index-map) %)]
             (hash-map
              :file-index file-index
              :row-index (- % (get (indexes :offsets) file-index))

              )))


    (group-by :file-index)
    (map
     (fn [[file-index row-indeces]]
       (let [ds (arrow/read-stream-dataset-inplace (nth fnames file-index))]
         (ds/select ds :all (map :row-index row-indeces)))))
    (reduce ds/concat))
    )


;;;  implement all of PColumnar dataset
  Counted
  (count [this]
    (->>
     (map
      #(arrow/read-stream-dataset-inplace %)
      fnames)
     (map ds/row-count)
     (reduce +)

     ))
  dtype-proto/PShape
  (shape [m]
    [
     (ds/column-count
      (arrow/read-stream-dataset-inplace (first fnames)))
     (count m)
     ]))



(defn filter-and-collect
  ([fnames predicate]
   (->> fnames
        (map
         #(let [dataset (arrow/read-stream-dataset-inplace %)
                reader (ds-readers/mapseq-reader dataset)]
            (ds/select dataset :all (argops/argfilter predicate reader))))
        (reduce ds/concat-copying)
        )))


;; (defn my-filter-1-oom
;;   ([fnames predicate]
;;    (->> fnames
;;         (map
;;          #(let [dataset (arrow/read-stream-dataset-inplace %)
;;                 reader (ds-readers/mapseq-reader dataset)]
;;             (ds/select dataset :all (argops/argfilter predicate reader))))
;;         (reduce ds/concat))))


;; (defn my-filter-2-oom
;;   ([fnames predicate]
;;    (let [one-ds (->> fnames
;;                      (map arrow/read-stream-dataset-inplace)
;;                      (reduce ds/concat))]
;;      (ds/filter one-ds predicate))))

;; (defn my-filter-3-oom
;;   ([fnames indexes predicate]
;;    (let [
;;          directory-ds (->DirectoryDS fnames indexes)
;;          ]
;;      (ds/filter directory-ds predicate))))


(comment

  ;; usage
  (def fnames (->> (file-seq (io/file "/home/carsten/Dropbox/sources/tablecloth/allscreenings"))
                   (filter #(.isFile %))
                   (map #(.getPath %))
                   ))



  (def indexes (make-indexes fnames))     ;; to make ds/select work efficiently
  (def dss (DirectoryDS. fnames indexes)) ;;  construct directory based dataset
  (ds/row-count dss)
  ;;; 11 million rows, takes 40 seconds


;;;  ds/filter currenty pulls in all data, so gives OOM
;;;  for now it does not work with DirectoryDS
;;;
;;;
;;; This is my major use case
;;; This works on arbitray lage arrow collections
;;; (as long as the "filter result per file" fits in memory
;;; (and the overall filter result fits in memory)
;;; It might get slow, but no GC pressure at all
;;;
  (def search-result
    (time
     (->>
      (filter-and-collect fnames #(= "5" (get % "refid"))))))
  ;; takes 50 seconds
  (println search-result)




  ;; (def big (arrow/read-stream-dataset-inplace "/home/carsten/tmp/big.arrow"))

  ;; (ds/filter big #(= "no-cc" (get % "license")))
  )
