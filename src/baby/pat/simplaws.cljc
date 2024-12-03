(ns baby.pat.simplaws
  (:require [baby.pat.secrets :as !]
            [baby.pat.vt :as vt]
            [orchestra.core #?(:clj :refer :cljs :refer-macros) [defn-spec]]
            [clojure.spec.alpha :as s]
            [camel-snake-kebab.core :as csk]
            [camel-snake-kebab.extras :as cske]
            #?(:bb [com.grzm.awyeah.client.api :as aws]
               :clj [cognitect.aws.client.api :as aws])
            #?(:bb [com.grzm.awyeah.credentials :as credentials]
               :clj [cognitect.aws.credentials :as credentials]))
  #?(:bb (:import [java.io])
     :clj (:import com.amazonaws.services.sqs.AmazonSQSClientBuilder
                   [com.amazonaws.auth AWSStaticCredentialsProvider]
                   [com.amazonaws.auth BasicAWSCredentials])))

(def ops aws/ops)

(s/def ::vt/cognitect-aws-client #(baby.pat.vt/type-of? % "cognitect.aws.client.impl.Client"))

(defn-spec quick-client ::vt/cognitect-aws-client [variant ::vt/qkw]
  "Takes a variant and returns an aws client for that variant to use."
  (let [api (namespace variant)
        secret (fn [s] (!/get-secret (keyword (str api "." (name variant)) s)))
        r (secret "region")
        aki (secret "access-key-id")
        sak (secret "secret-access-key")]
    (aws/client
     {:api (if (= api "ses") :sesv2 (keyword api))
      :region r
      :credentials-provider (credentials/basic-credentials-provider
                             {:access-key-id aki
                              :secret-access-key sak})})))

#?(:bb (defn sqs-quick-consumer-client [variant] nil)
   :clj (defn-spec sqs-quick-consumer-client ::vt/any [variant ::vt/qkw]
          "In order to use squeedo I need a different type of client. This client works with sqs only."
          (let [api (namespace variant)
                secret (fn [s] (!/get-secret (keyword (str api "." (name variant)) s)))
                r (secret "region")
                aki (secret "access-key-id")
                sak (secret "secret-access-key")]
            (-> (AmazonSQSClientBuilder/standard)
                (.withRegion r)
                (.withCredentials
                 (AWSStaticCredentialsProvider. (BasicAWSCredentials. aki sak)))
                .build))))

(def s3 (quick-client :s3/default))
(def ses (quick-client :ses/default))
(def sns (quick-client :sns/default))
(def sqs (quick-client :sqs/default))
(def sqs-consumer #?(:bb (quick-client :sqs/default)
                     :clj (sqs-quick-consumer-client :sqs/default)))


(def aws-clients-raw (mapv (fn [variant]
                             (case (namespace variant)
                               "sqs-consumer" {:aws-client/id variant
                                               :aws-client/aws-client (sqs-quick-consumer-client :sqs/default)}
                               {:aws-client/id variant
                                :aws-client/aws-client (quick-client variant)}))
                           #{:s3/default :ses/default :sns/default :sqs/default :sqs-consumer/default}))

(def aws-clients
  "Clients that are available to use AWS resources."
  (vt/add {} aws-clients-raw))

(defn retrieve-client [universe variant]
  "Returns a client associated with a variant. Helper fn."
  (vt/<- universe [:aws-client/id variant :aws-client/aws-client]))

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
Add an ops map and you'll get an operation on the provided variant.
Those variants could map to any client you provide if it's normalized in the supplied universe.
Otherwise you're stuck with the defaults."
  ([variant ::vt/any] (aws/ops (retrieve-client aws-clients variant)))
  ([variant ::vt/any op ::vt/map] (invoke aws-clients variant op))
  ([universe ::vt/map variant ::vt/any op ::vt/map]
   (let [client (retrieve-client universe variant)
         _ (tap> {:client client
                  :uni universe
                  :var variant})
         op (medley.core/map-kv (fn [k v]
                                  [k (if (map? v)
                                       (cske/transform-keys csk/->PascalCaseKeyword v)
                                       (if (keyword? v)
                                         (csk/->PascalCaseKeyword v)
                                         v))])
                                op)
         res (-> (aws/invoke client op)
                 (throw-anomaly {:variant variant :op op}))]
     (cske/transform-keys csk/->kebab-case-keyword res))))

(comment

(defn ps [kw]
  (let [secret (fn [nm] (!/get-secret (keyword (str "aws." (name kw)) nm)))
        kn (fn [nm] (keyword (str (name kw) ".default") nm))
        rv (secret "region")
        aki (secret "access-key-id")
        sak (secret "secret-access-key")]
    (doall [(!/add-secret! (kn "region") rv)
            (!/add-secret! (kn "access-key-id") aki)
            (!/add-secret! (kn "secret-access-key") sak)])))

  )
