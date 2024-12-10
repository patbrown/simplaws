(ns baby.pat.simplaws.clients
  (:require [baby.pat.secrets :as !]
            [baby.pat.nm :as nm]
            [baby.pat.vt :as vt]
            [orchestra.core #?(:clj :refer :cljs :refer-macros) [defn-spec]]
            #?(:bb [com.grzm.awyeah.client.api :as aws]
               :clj [cognitect.aws.client.api :as aws])
            #?(:bb [com.grzm.awyeah.credentials :as credentials]
               :clj [cognitect.aws.credentials :as credentials]))
  #?(:bb (:import [java.io])
     :clj (:import com.amazonaws.services.sqs.AmazonSQSClientBuilder
                   [com.amazonaws.auth AWSStaticCredentialsProvider]
                   [com.amazonaws.auth BasicAWSCredentials])))

;; TODO spec for clients

(defn-spec quick-client ::vt/any [variant ::vt/qkw]
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
  (nm/add {} aws-clients-raw))

(defn retrieve-client [nm variant]
  "Returns a client associated with a variant. Helper fn."
  (nm/<- nm [:aws-client/id variant :aws-client/aws-client]))


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
