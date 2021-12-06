(ns jepsen.paxos
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen
             [cli :as cli]
             [client :as client]
             [control :as c]
             [db :as db]
             [generator :as gen]
             [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(use 'clojure.string)
(use
  '[clojure.java.shell :only [sh]])

(defn call-shell
  [& args]
  (let [outcome (apply sh args)]
    (if (= 0 (:exit outcome))
      (trim (:out outcome))
      (throw (Exception. (:err outcome))))))

(defn rsync
  "Rsync source files to destination. (Because Jepsen's upload doesn't support recursive, I think.)"
  [& args]
  (apply call-shell
         (concat
          (list "rsync" "-e" "/usr/bin/ssh '-o StrictHostKeyChecking=no'" "-az")
          args)))

(defn db
  "Not a DB, a Paxos node."
  []
  (reify
   db/DB
   (setup! [_ test node]
           (info "installing Python-Paxos, rsyncing code and config")
           ; Upload this code and config file to worker node.
           (rsync "/home/admin/python-paxos-jepsen/"
                  (str node ":python-paxos-jepsen"))
           (rsync "../../../nodes" (str node ":"))
           ; Upload Python 3.9. See README for building Python.
           ; TODO: README instructions
           (rsync "/home/admin/python3.9" (str node ":"))
           ; Executed on the remote worker node.
           (info "pip-installing requirements")
           (c/exec "/home/admin/python3.9/bin/pip3" "install" "-r"
                   "/home/admin/python-paxos-jepsen/paxos/requirements.txt")
           (info "starting daemon")
           (c/su
            (c/exec "/bin/bash" "/home/admin/python-paxos-jepsen/start-daemon.sh"))
           (Thread/sleep 10000))
   (teardown! [_ test node]
              (info node "tearing down Paxos")
              (c/su
               (c/exec "/sbin/start-stop-daemon" "--stop" "--pidfile" "/var/paxos.pid" "--exec"
                       "/home/admin/python3.9/bin/python3.9" "--oknodo")))

   db/LogFiles
   (log-files [_ test node]
              ["/home/admin/paxos.log"])))

(defn paxos-client-write
  [value]
  (call-shell "/home/admin/python3.9/bin/python3.9"
              "/home/admin/python-paxos-jepsen/paxos/client.py"
              "/home/admin/nodes" (str value)))

(defrecord Client [conn]
  client/Client
  (open! [this test node] this)
  (setup! [this test])
  (invoke! [this test op]
    (println "op" op)
    (case (:f op) :write (do
                           (info (paxos-client-write (:value op)))
                           (assoc op :type :ok))))
  (teardown! [this test])
  (close! [_ test]))

(defn write-op [_ _] {:type :invoke, :f :write, :value 1})

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
          :client          (Client. nil)
          :generator       (->> write-op
                                (gen/stagger 1)
                                (gen/nemesis nil)
                                (gen/time-limit 15))
          :pure-generators true}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run!
   (merge (cli/single-test-cmd {:test-fn paxos-test})
          (cli/serve-cmd))
   args))
