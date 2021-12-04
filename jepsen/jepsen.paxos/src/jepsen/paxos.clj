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

(use
  '[clojure.java.shell :only [sh]])

(defn db
  "Not a DB, a Paxos node."
  []
  (reify
   db/DB
   (setup! [_ test node]
           (info node "installing Python-Paxos")
           ; Upload Paxos implementation's Python code.
           ; I can't figure out how to scp -r the local code, so tar it locally and untar on remote.
           (sh "rm" "-f" "/tmp/paxos.tar.gz")
           (sh "tar" "czf" "/tmp/paxos.tar.gz" "../../paxos")
           ; c/exec executes on each remote node.
           (c/exec "rm" "-f" "paxos.tar.gz")
           (c/upload "/tmp/paxos.tar.gz" "paxos.tar.gz")
           (c/exec "tar" "-C" "~" "-xzf" "~/paxos.tar.gz")
           ; Upload Python 3.9. See README for building Python.
           ; TODO: README instructions
           (c/upload "/home/admin/python3.9.tar.gz" "python3.9.tar.gz")
           (c/exec "tar" "--directory" "~" "-xzf" "/home/admin/python3.9.tar.gz")
           (c/exec "/home/admin/python3.9/bin/pip3" "install" "-r" "/home/admin/paxos/requirements.txt"))

   (teardown! [_ test node]
              (info node "tearing down Paxos (TODO)"))))

(defn paxos-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (info "paxos-test")
  (merge tests/noop-test
         opts
         {:name            "paxos"
          :os              debian/os
          :db              (db)
          :pure-generators true}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run!
   (merge (cli/single-test-cmd {:test-fn paxos-test})
          (cli/serve-cmd))
   args))
