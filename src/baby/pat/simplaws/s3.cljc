(ns baby.pat.simplaws.s3
  (:require [orchestra.core #?(:clj :refer :cljs :refer-macros) [defn-spec]]
            [baby.pat.simplaws :as aws :refer [aws-clients]]
            [baby.pat.vt :as vt]))

(defn-spec *list-buckets ::vt/map
  ([] (*list-buckets aws-clients :s3/default))
  ([variant ::vt/qkw]
   (*list-buckets aws-clients variant))
  ([universe ::vt/map variant ::vt/qkw]
   (aws/invoke universe variant {:op :list-buckets})))

(defn-spec list-buckets ::vt/vec-of-maps
  "Lists buckets associated with supplied variant or default if unprovided."
  ([] (list-buckets aws-clients :s3/default))
  ([variant ::vt/qkw] (list-buckets aws-clients variant))
  ([universe ::vt/map variant ::vt/qkw]
   (->> (*list-buckets universe variant)
        :buckets
        (map :name)
        set)))

(defn-spec bucket-exists? ::vt/?
  "Does the provided bucket exist inside the provided client?"
  ([bucket ::vt/qkw] (bucket-exists? aws-clients :s3/default bucket))
  ([variant ::vt/qkw bucket ::vt/qkw] (bucket-exists? aws-clients variant bucket))
  ([universe ::vt/map variant ::vt/qkw bucket ::vt/str]
   (contains? (list-buckets universe variant) bucket)))

(defn-spec create-bucket ::vt/discard
  ([bucket ::vt/str] (create-bucket aws-clients :s3/default bucket))
  ([variant ::vt/qkw bucket ::vt/str] (create-bucket aws-clients variant bucket))
  ([universe ::vt/map variant ::vt/qkw bucket ::vt/str]
   (aws/invoke universe variant {:op      :create-bucket
                                 :request {:bucket bucket}})))

(defn-spec list-objects ::vt/set-of-strs
  "Returns a set of strings of available objects in a bucket"
  ([bucket ::vt/str] (list-objects aws-clients :s3/default bucket))
  ([variant ::vt/qkw bucket ::vt/str] (list-objects aws-clients variant bucket))
  ([universe ::vt/map variant ::vt/qkw bucket ::vt/str]
   (when (bucket-exists? universe variant bucket)
     (let [resp (:contents (aws/invoke universe variant {:op      :list-objects
                                                         :request {:bucket bucket}}))]
       (set (mapv :key resp))))))

(defn-spec object-exists? ::vt/?
  "Does the object exist in the bucket inside the client variant?"
  ([bucket ::vt/str object ::vt/str] (object-exists? aws-clients :s3/default bucket object))
  ([variant ::vt/qkw bucket ::vt/str object ::vt/str] (object-exists? aws-clients :s3/default bucket object))
  ([universe ::vt/map variant ::vt/qkw bucket ::vt/str object ::vt/str]
   (contains? (list-objects universe variant bucket) object)))

(defn-spec put-object ::vt/discard
  "Puts an object in a bucket."
  ([bucket ::vt/str object ::vt/str body ::vt/any]
   (put-object aws-clients :s3/default bucket object body))
  ([variant ::vt/qkw bucket ::vt/str object ::vt/str body ::vt/any]
   (put-object aws-clients variant bucket object body))
  ([universe ::vt/map variant ::vt/qkw bucket ::vt/str object ::vt/str body ::vt/any]
   (let [_ (tap> {:u universe :v variant :b bucket :o object :body body})
         _ (when-not (bucket-exists? universe variant bucket)
             (create-bucket universe variant bucket))]
     (aws/invoke universe variant {:op :put-object :request {:bucket bucket
                                                             :key object
                                                             :body body}}))))

(defn-spec get-object ::vt/any
  "Returns the body of an object in a bucket, but does no further response modifications."
  ([bucket ::vt/str object ::vt/str] (get-object aws-clients :s3/default bucket object))
  ([variant ::vt/qkw bucket ::vt/str object ::vt/str] (get-object aws-clients variant bucket object))
  ([universe ::vt/map variant ::vt/qkw bucket ::vt/str object ::vt/str]
   (let [_ (tap> {:var variant :bucket bucket :obj object})
         _ (when-not (object-exists? universe variant bucket object)
             (put-object universe variant bucket object nil))]
     (:body (aws/invoke universe variant {:op :get-object :request {:bucket bucket
                                                                    :key object}})))))

(defn-spec delete-object ::vt/any
  "Removes an object from a bucket."
  ([bucket ::vt/str object ::vt/str] (delete-object aws-clients :s3/default bucket object))
  ([variant ::vt/qkw bucket ::vt/str object ::vt/str] (delete-object aws-clients variant bucket object))
  ([universe ::vt/map variant ::vt/qkw bucket ::vt/str object ::vt/str]
   (aws/invoke universe :s3 {:op :delete-object :request {:bucket bucket
                                                          :key object}})))

(comment

(msg/read-message bc-stream)
(def bc-stream (get-object-as-email "bucket-ses-pat-baby-testing-flow" "4adit8i1cabrnf7jsfaoea46678668iuii6hu881"))
bc-stream

;;
  )
