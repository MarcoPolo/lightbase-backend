(ns websocks.core
  (:require [clojure.data.json :as json]
            [clojurewerkz.welle.core :as wc]
            [clojurewerkz.welle.buckets :as wb]
            [clojurewerkz.welle.kv :as kv]
            [clojure.string :as s])
  (:import [org.webbitserver WebServer WebServers WebSocketHandler]
           [org.webbitserver.handler StaticFileHandler]
           com.basho.riak.client.http.util.Constants))


(wc/connect!)
(wb/create "lightbases")

;; Store data that maps lightbase name to their connections
(def lightbase-client (atom {}))
;; And another to find the lightbase name from their connection
(def client-lightbase (atom {}))

lightbase-client

;; Blast a message to all subscribers of a lightbase 
(defn update-clients [lightbase data]
  (pmap 
    #(.send 
       %
       (json/json-str {:type "updateclient" :message data }))
    (lightbase @lightbase-client #{})))

(kv/store "lightbases" (str :asdf ) "hello" :content-type Constants/CTYPE_TEXT_UTF8)
(kv/fetch-one "lightbases" (str :asdf))

;; When we get a new message from one of the clients lets go here
(defn on-message [conn json-msg]
  (def connection conn)
  (println "Got message" json-msg)
  (let [message (json/read-json json-msg)
        message-type (:type message)
        message-data (:data message)
        lightbase (keyword (:lightbase message-data))]
    (println "type" message-type "data" (:message message-data))
    (condp = message-type
      ;; If a client wants to register to a lightbase
      "register" (do 
                   (swap! lightbase-client #(update-in % [lightbase] clojure.set/union #{conn}))
                   (swap! client-lightbase assoc conn lightbase)
                   (.send 
                     conn 
                     (json/json-str {:type "updateclient" :message (:value (kv/fetch-one "lightbases" (str lightbase)))})))
      ;; If a client wants to update the lighbase
      "update" (do
                 (println "saving to lightbases" (str lightbase) (:message message-data))
                 (kv/store "lightbases" (str lightbase) (:message message-data) :content-type Constants/CTYPE_TEXT_UTF8)
                 (.send conn (json/json-str {:type "updateclient" :message (:message message-data)}))
                 (future (update-clients lightbase (:message message-data)))))))
          

;; Get rid off the client from our map
(defn on-close [conn]
  (println "Client disconnected")
  (let [lightbase (@client-lightbase conn)]
    (swap! client-lightbase dissoc conn)
    (swap! lightbase-client #(update-in % [lightbase] clojure.set/difference #{conn}))))


;(.stop ws)

(defn -main []
   
  (def ws (WebServers/createWebServer 8080))

  (doto ws
    (.add "/websocket"
          (proxy [WebSocketHandler] []
            (onOpen [c] (println "Client connected"))
            (onClose [c] (on-close c))
            (onMessage [c j] (on-message c j))))

    (.add (StaticFileHandler. "."))
    (.start))
  )

