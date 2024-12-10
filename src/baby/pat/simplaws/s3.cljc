(ns baby.pat.simplaws.s3
  (:require [orchestra.core #?(:clj :refer :cljs :refer-macros) [defn-spec]]
            [baby.pat.simplaws :as aws]
            [baby.pat.jes.vt :as vt]))

(defn-spec list-buckets ::vt/vec-of-maps
  "Lists buckets associated with supplied variant or default if unprovided."
  ([client ::vt/any]
   (->> (aws/invoke client {:op :list-buckets})
        :buckets
        (map :name)
        set)))

(defn-spec bucket-exists? ::vt/?
  "Does the provided bucket exist inside the provided client?"
  [client ::vt/any bucket ::vt/str]
  (contains? (list-buckets client) bucket))

(defn-spec create-bucket ::vt/discard
  [client ::vt/any bucket ::vt/str]
  (aws/invoke client {:op      :create-bucket
                      :request {:bucket bucket}}))

(defn-spec list-objects ::vt/set-of-strs
  "Returns a set of strings of available objects in a bucket"
  [client ::vt/any bucket ::vt/str]
  (when (bucket-exists? client bucket)
    (let [resp (:contents (aws/invoke client {:op      :list-objects
                                              :request {:bucket bucket}}))]
      (set (mapv :key resp)))))

(defn-spec object-exists? ::vt/?
  "Does the object exist in the bucket inside the client variant?"
  ([client ::vt/any bucket ::vt/str object ::vt/str]
   (contains? (list-objects client bucket) object)))

(defn-spec put-object ::vt/discard
  "Puts an object in a bucket."
  ([client ::vt/any bucket ::vt/str object ::vt/str body ::vt/any]
   (let [_ (when-not (bucket-exists? client bucket)
             (create-bucket client bucket))]
     (aws/invoke client {:op :put-object :request {:bucket bucket
                                                   :key object
                                                   :body body}}))))

(defn-spec get-object ::vt/any
  "Returns the body of an object in a bucket, but does no further response modifications."
  ([client ::vt/any bucket ::vt/str object ::vt/str]
   (let [_ (when-not (object-exists? client bucket object)
             (put-object client bucket object nil))]
     (:body (aws/invoke client {:op :get-object :request {:bucket bucket
                                                          :key object}})))))

(defn-spec delete-object ::vt/any
  "Removes an object from a bucket."
  ([client ::vt/any bucket ::vt/str object ::vt/str]
   (aws/invoke client {:op :delete-object :request {:bucket bucket
                                                    :key object}})))

(comment

(msg/read-message bc-stream)
(def bc-stream (get-object-as-email "bucket-ses-pat-baby-testing-flow" "4adit8i1cabrnf7jsfaoea46678668iuii6hu881"))
bc-stream

;;
  )
