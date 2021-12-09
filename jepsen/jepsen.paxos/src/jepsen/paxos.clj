(ns jepsen.paxos
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen
             [checker :as checker]
             [cli :as cli]
             [client :as client]
             [control :as c]
             [db :as db]
             [generator :as gen]
             [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [knossos.model :as model])
  (:import (knossos.model Model)))

(use
 '[clojure.string :only [trim]])
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
          ; The usual rsync -i doesn't work for me on Debian 10? Use rsync -e "ssh -i ...".
          (list "rsync" "-e" "/usr/bin/ssh -o StrictHostKeyChecking=no -i /home/admin/.ssh/python-paxos-jepsen.pem"
                "-az")
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
           (info "pip-installing requirements")
           ; Local pip install, for client.py.
           (call-shell "python3.9" "-m" "pip" "install" "-r"
                       "/home/admin/python-paxos-jepsen/paxos/requirements.txt")
           ; Executed on the remote worker node.
           (c/exec "python3.9" "-m" "pip" "install" "-r"
                   "/home/admin/python-paxos-jepsen/paxos/requirements.txt")
           (info "starting daemon")
           (c/su
            (c/exec "/bin/bash" "/home/admin/python-paxos-jepsen/start-daemon.sh"))
           (Thread/sleep 10000))

   (teardown! [_ test node]
              (info node "tearing down Paxos")
              (c/su
               (c/exec "/sbin/start-stop-daemon" "--stop" "--pidfile" "/var/paxos.pid" "--exec"
                       "/usr/local/bin/python3.9" "--oknodo")))

   db/LogFiles
   (log-files [_ test node]
              ["/home/admin/paxos.log"])))

(defn paxos-client-append
  "Append value to the shared state (a vector of ints) and return the new state."
  [process-id value nodes-count]
  (json/read-str
   (call-shell "/usr/local/bin/python3.9"
               "/home/admin/python-paxos-jepsen/paxos/client.py"
               "/home/admin/nodes" "--server" (str (mod process-id nodes-count)) (str value))))

(defrecord Client [conn]
  client/Client
  (open! [this test node] this)
  (setup! [this test])
  (invoke! [this test op]
    ; Append is the only operation. The input int (:value op) is appended to the shared state.
    ; The new value and new shared state (from the server reply) are stored as the new :value
    ; for the sake of the AppendableList model, below.
    (assert (= (:f op) :append))
    (let [new-state (paxos-client-append (:process op) (:value op) (count (:nodes test)))]
      (assoc op :type :ok, :value {:new-state new-state :appended-value (:value op)})))
  (teardown! [this test])
  (close! [_ test]))

(defn append-op [_ _]
  ; TODO: unique values.
  {:type :invoke, :f :append, :value (rand-int 10000000)})

(defn vec-slice
  "Subvec with bounds clamping."
  [vec start end]
  (subvec vec (min start (count vec)) (min end (count vec))))

; A Knossos model, validates that the Paxos system's state (which is an appendable vector of ints)
; behaves as it ought.
(defrecord AppendableList [state]
  Model
  (step [model op]
    (assert (= (:f op) :append))
    (if (nil? (:new-state (:value op)))
      ; op failed.
      (do (info "failed" op) model)
      ; op succeeded. E.g., if state is [1 2] and we append 3, and the reply is [1 2 3 4] because
      ; another process appended 4, then op is {:value {:appended-value 3, :new-state [1 2 3 4]}}.
      ; Linearizability demands that [1 2] is a prefix of new-state and 3 is in the suffix.
      (let [appended-value (:appended-value (:value op))
            new-state      (:new-state (:value op))
            actual-prefix  (vec-slice new-state 0 (count state))
            actual-suffix  (vec-slice new-state (count state) (count new-state))]
        (info
          (str "state: " state "appended-value: " appended-value ", new-state: " new-state ", actual-prefix: " actual-prefix ", actual-suffix: " actual-suffix ", " (some #(= appended-value %) actual-suffix)))
        (cond
          (< (count new-state) (count state))
          (knossos.model/inconsistent
            (str "new state: " new-state " shorter than state: " state))
          (not= state actual-prefix)
          (knossos.model/inconsistent
            (str "state: " state "not a prefix of new state: " new-state))
          (not (some #(= appended-value %) actual-suffix))
          (knossos.model/inconsistent
            (str "appended value: " appended-value " not in new values: " actual-suffix))
          :else
          (AppendableList. new-state))))))

(defn appendable-list
  "Make an empty AppendableList."
  []
  (AppendableList. []))

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
          ; TODO: try nemesis.
          :generator       (->> append-op
                                (gen/stagger 1/20)
                                (gen/nemesis nil)
                                (gen/time-limit 3))
          ; Use Knossos checker because it's in the tutorial. TODO: try Elle.
          :checker         (checker/compose
                            {:linear   (checker/linearizable
                                        {:model     (appendable-list)
                                         :algorithm :linear})
                             :timeline (timeline/html)})
          :pure-generators true}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run!
   (merge (cli/single-test-cmd {:test-fn paxos-test})
          (cli/serve-cmd))
   args))
