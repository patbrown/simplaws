(ns baby.pat.simplaws.sqs
  (:require [clojure.spec.alpha :as s]
            [clojure.string]
            [orchestra.core #?(:clj :refer :cljs :refer-macros) [defn-spec]]
            [baby.pat.jes.vt :as vt]
            [baby.pat.jes.vt.util :as u]
            [baby.pat.simplaws :as aws]))

(defn-spec *list-queues ::vt/map
  ([client ::vt/any]
   (aws/invoke client {:op :list-queues})))

(defn-spec list-queues ::vt/vec-of-strs
  "Lists queues associated with the given variant"
  ([client ::vt/any]
   (:queue-urls (*list-queues client))))

(defn-spec url-for-queue ::vt/str
  "Returns a string url of a queue."
  ([client ::vt/any queue-id ::vt/qkw-or-str]
   (let [queue-id (u/ensure-safe-string queue-id)]
     (first (filter #(clojure.string/ends-with? % queue-id) (list-queues client))))))

(defn-spec arn-from-queue-url ::vt/str
  "Returns the ARN of a queue for a given URL"
  ([client ::vt/any url ::vt/str]
   (let [res (aws/invoke client {:op :get-queue-attributes
                                           :request {:queue-url url
                                                     :attribute-names ["QueueArn"]}})]
     (-> res :attributes :queue-arn))))

(defn-spec arn-for-queue ::vt/str
  "Given a string id of a queue returns the string arn."
  ([client ::vt/any queue-id ::vt/qkw-or-str]
   (let [queue-id (u/ensure-safe-string queue-id)
         url (url-for-queue client queue-id)]
     (arn-from-queue-url client url))))

(defn-spec create-queue! ::vt/map
  "Creates a new topic with the given name if it does not exist. Always returns the topic arn."
  ([client ::vt/any queue-id ::vt/qkw-or-str]
   (let [{:keys [queue-url] :as yuck} (aws/invoke client {:op :create-queue :request {:queue-name queue-id}})
         queue-id (u/ensure-safe-string queue-id)
         queue-arn (arn-for-queue client queue-id)]
     {:queue/id queue-id
      :queue/url queue-url
      :queue/arn queue-arn})))

(s/def ::vt/queue-send-response (s/and ::vt/map #(baby.pat.vt/contains-keys? % :md-5-of-message-body :message-id)))

(defn-spec send! ::vt/queue-send-response
  "Sends a message to a queue"
  ([client ::vt/any queue-id ::vt/qkw-or-str message ::vt/str]
   (aws/invoke client {:op :send-message
                                 :request {:message-body message
                                           :queue-url (url-for-queue client queue-id)}})))
