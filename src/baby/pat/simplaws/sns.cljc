(ns baby.pat.simplaws.sns
  (:require [clojure.string]
            [orchestra.core #?(:clj :refer :cljs :refer-macros) [defn-spec]]
            [baby.pat.jes.vt :as vt]
            [baby.pat.jes.vt.util :as u]
            [baby.pat.simplaws :as aws]))

(defn-spec *list-topics ::vt/map
  ([client ::vt/any]
   (aws/invoke client {:op :list-topics})))

(defn-spec list-topics ::vt/vec-of-maps
  "Lists abailable topics inside the variant."
  ([client ::vt/any]
   (->> (*list-topics client)
        :topics
        (map :topic-arn)
        (mapv (fn [t] {:topic/id (u/safe-string->kw (last (clojure.string/split t #":")))
                       :topic/arn t})))))

(defn-spec create-topic! ::vt/map
  "Creates a topic from a string"
  ([client ::vt/any topic-id ::vt/qkw-or-str]
   (let [topic-id-str (u/ensure-safe-string topic-id)
         {:keys [topic-arn]} (aws/invoke client {:op :create-topic :request {:name topic-id-str}})]
     {:topic/id topic-id
      :topic/arn topic-arn})))

(defn-spec <-arn ::vt/str
  "Given a topic-id returns the ARN associated with the topic."
  ([client ::vt/any topic-id ::vt/qkw]
   (:topic/arn (first (filter (fn [t] (= (:topic/id t) topic-id)) (list-topics client))))))

(defn-spec publish! ::vt/map
  "Sends a message string to the given topic."
  ([client ::vt/any id ::vt/str message ::vt/str]
   (aws/invoke client {:op :publish
                       :request (merge {:topic-arn (<-arn client id)
                                        :message message})})))

#_(publish! :testing-whirlwind/returns {:your/id :your/cool})
