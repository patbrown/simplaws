(ns baby.pat.simplaws
  (:require [baby.pat.secrets :as !]
            [baby.pat.jes.vt :as vt]
            [baby.pat.jes.vt.util :as u]
            [orchestra.core #?(:clj :refer :cljs :refer-macros) [defn-spec]]
            [clojure.spec.alpha :as s]
            [camel-snake-kebab.core :as csk]
            [camel-snake-kebab.extras :as cske]
            #?(:bb [com.grzm.awyeah.client.api :as aws]
               :clj [cognitect.aws.client.api :as aws])
            #?(:bb [com.grzm.awyeah.credentials :as credentials]
               :clj [cognitect.aws.credentials :as credentials])))

(def ops aws/ops)

(s/def ::vt/cognitect-aws-client #(u/type-of? % "cognitect.aws.client.impl.Client"))

(defn throw-anomaly
  "Checks aws/invoke response, if it contains an anomaly, throw
  a exception with the error, if it is ok, return the response."
  [resp info-map]
  (if (get resp :cognitect.anomalies/category)
    (throw (ex-info "A error was returned by aws/invoke " {:error-resp resp
                                                           :info info-map}))
    resp))

(defn-spec invoke ::vt/any
  "Candy around aws invoke which makes life much easier.   
   A variant alone returns a map of available operations.   
   Add an ops map and you'll get an operation on the provided client.   
   The magic is in everything looking very Clojure-like with camel case and sets etc."   
  [client ::vt/any op ::vt/map]
  (let [op (medley.core/map-kv (fn [k v]
                                 [k (if (map? v)
                                      (cske/transform-keys csk/->PascalCaseKeyword v)
                                      (if (keyword? v)
                                        (csk/->PascalCaseKeyword v)
                                        v))])
                               op)
        res (-> (aws/invoke client op)
                (throw-anomaly {:client client :op op}))]
    (cske/transform-keys csk/->kebab-case-keyword res)))
