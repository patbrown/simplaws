(ns baby.pat.simplaws.sqs
  (:require [clojure.spec.alpha :as s]
            [clojure.string]
            [orchestra.core #?(:clj :refer :cljs :refer-macros) [defn-spec]]
            [baby.pat.vt :as vt]
            [baby.pat.simplaws :as aws :refer [aws-clients]]))

(defn-spec *list-queues ::vt/map
  ([] (*list-queues aws-clients :sqs/default))
  ([variant ::vt/qkw] (*list-queues aws-clients variant))
  ([universe ::vt/map variant ::vt/qkw]
   (aws/invoke universe variant {:op :list-queues})))

(defn-spec list-queues ::vt/vec-of-strs
  "Lists queues associated with the given variant"
  ([] (list-queues aws-clients :sqs/default))
  ([variant ::vt/qkw] (list-queues aws-clients variant))
  ([universe ::vt/map variant ::vt/qkw]
   (:queue-urls (*list-queues universe variant))))

(defn-spec url-for-queue ::vt/str
  "Returns a string url of a queue."
  ([queue-id ::vt/qkw-or-str] (url-for-queue aws-clients :sqs/default queue-id))
  ([variant ::vt/qkw queue-id ::vt/qkw-or-str] (url-for-queue aws-clients variant queue-id))
  ([universe ::vt/map variant ::vt/qkw queue-id ::vt/qkw-or-str]
   (let [queue-id (vt/ensure-safe-string queue-id)]
     (first (filter #(clojure.string/ends-with? % queue-id) (list-queues universe variant))))))

(defn-spec arn-from-queue-url ::vt/str
  "Returns the ARN of a queue for a given URL"
  ([url ::vt/str] (arn-from-queue-url aws-clients :sqs/default url))
  ([variant ::vt/qkw url ::vt/str] (arn-from-queue-url aws-clients variant url))
  ([universe ::vt/map variant ::vt/qkw url ::vt/str]
   (let [res (aws/invoke universe variant {:op :get-queue-attributes
                                           :request {:queue-url url
                                                     :attribute-names ["QueueArn"]}})]
     (-> res :attributes :queue-arn))))

(defn-spec arn-for-queue ::vt/str
  "Given a string id of a queue returns the string arn."
  ([queue-id ::vt/qkw-or-str] (arn-for-queue aws-clients :sqs/default queue-id))
  ([variant ::vt/qkw queue-id ::vt/qkw-or-str] (arn-for-queue aws-clients variant queue-id))
  ([universe ::vt/map variant ::vt/qkw queue-id ::vt/qkw-or-str]
   (let [queue-id (vt/ensure-safe-string queue-id)
         url (url-for-queue universe variant queue-id)]
     (arn-from-queue-url universe variant url))))

(defn-spec create-queue! ::vt/map
  "Creates a new topic with the given name if it does not exist. Always returns the topic arn."
  ([queue-id ::vt/qkw-or-str] (create-queue! aws-clients :sqs/default queue-id))
  ([variant ::vt/qkw queue-id ::vt/qkw-or-str] (create-queue! aws-clients :sqs/default queue-id))
  ([universe ::vt/map variant ::vt/qkw queue-id ::vt/qkw-or-str]
   (let [{:keys [queue-url] :as yuck} (aws/invoke universe variant {:op :create-queue :request {:queue-name queue-id}})
         queue-id (vt/ensure-safe-string queue-id)
         queue-arn (arn-for-queue universe variant queue-id)]
     {:queue/id queue-id
      :queue/url queue-url
      :queue/arn queue-arn})))

(s/def ::vt/queue-send-response (s/and ::vt/map #(baby.pat.vt/contains-keys? % :md-5-of-message-body :message-id)))

(defn-spec send! ::vt/queue-send-response
  "Sends a message to a queue"
  ([{:keys [queue message]} ::vt/map] (send! aws-clients :sqs/default queue message))
  ([queue-id ::vt/qkw-or-str message ::vt/str] (send! aws-clients :sqs/default queue-id message))
  ([variant ::vt/qkw queue-id ::vt/qkw-or-str message ::vt/str] (send! aws-clients variant queue-id message))
  ([universe ::vt/map variant ::vt/qkw queue-id ::vt/qkw-or-str message ::vt/str]
   (aws/invoke universe variant {:op :send-message
                                 :request {:message-body message
                                           :queue-url (url-for-queue universe variant queue-id)}})))
