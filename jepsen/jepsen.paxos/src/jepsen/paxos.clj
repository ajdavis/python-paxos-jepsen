(ns jepsen.paxos
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen
             [cli :as cli]
             [control :as c]
             [db :as db]
             [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(defn db
  "Not a DB, a Paxos node."
  []
  (reify
    db/DB
    (setup! [_ test node]
            (info node "installing Paxos" version))

    (teardown! [_ test node]
               (info node "tearing down Paxos"))))

(defn paxos-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         {:pure-generators true}
         opts))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run!
   (merge (cli/single-test-cmd {:test-fn paxos-test})
          (cli/serve-cmd))
   args))
