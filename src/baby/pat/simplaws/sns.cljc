(ns baby.pat.simplaws.sns
  (:require [clojure.string]
            [orchestra.core #?(:clj :refer :cljs :refer-macros) [defn-spec]]
            [baby.pat.vt :as vt]
            [baby.pat.simplaws :as aws :refer [aws-clients]]))

(defn-spec *list-topics ::vt/map
  ([] (*list-topics aws-clients :sns/default))
  ([variant ::vt/qkw] (*list-topics aws-clients variant))
  ([universe ::vt/map variant ::vt/qkw]
   (aws/invoke universe variant {:op :list-topics})))

(defn-spec list-topics ::vt/vec-of-maps
  "Lists abailable topics inside the variant."
  ([] (list-topics aws-clients :sns/default))
  ([variant ::vt/qkw] (list-topics aws-clients variant))
  ([universe ::vt/map variant ::vt/qkw]
   (->> (*list-topics universe variant)
        :topics
        (map :topic-arn)
        (mapv (fn [t] {:topic/id (vt/safe-string->kw (last (clojure.string/split t #":")))
                       :topic/arn t})))))

(defn-spec create-topic! ::vt/map
  "Creates a topic from a string"
  ([topic-id ::vt/qkw-or-str] (create-topic! aws-clients :sns/default topic-id))
  ([variant ::vt/qkw topic-id ::vt/qkw-or-str] (create-topic! aws-clients variant topic-id))
  ([universe ::vt/map variant ::vt/qkw topic-id ::vt/qkw-or-str]
   (let [topic-id-str (vt/ensure-safe-string topic-id)
         {:keys [topic-arn]} (aws/invoke universe variant {:op :create-topic :request {:name topic-id-str}})]
     {:topic/id topic-id
      :topic/arn topic-arn})))

(defn-spec <-arn ::vt/str
  "Given a topic-id returns the ARN associated with the topic."
  ([topic-id ::vt/qkw] (<-arn aws-clients :sns/default topic-id))
  ([variant ::vt/qkw topic-id ::vt/qkw] (<-arn aws-clients variant topic-id))
  ([universe ::vt/map variant ::vt/qkw topic-id ::vt/qkw]
   (:topic/arn (first (filter (fn [t] (= (:topic/id t) topic-id)) (list-topics universe variant))))))

(defn-spec publish! ::vt/map
  "Sends a message string to the given topic."
  ([id ::vt/str message ::vt/str] (publish! aws-clients :sns/default id message))
  ([variant ::vt/qkw id ::vt/str message ::vt/str] (publish! aws-clients variant id message))
  ([universe ::vt/map variant ::vt/qkw id ::vt/str message ::vt/str]
   (aws/invoke universe variant {:op :publish
                                 :request (merge {:topic-arn (<-arn universe variant id)
                                                  :message message #_(if-not (string? message)
                                                             (str message)
                                                             message)})})))

#_(publish! :testing-whirlwind/returns {:your/id :your/cool})
