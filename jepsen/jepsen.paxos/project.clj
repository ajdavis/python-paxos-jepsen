(defproject jepsen.paxos "0.1.0-SNAPSHOT"
  :description "Test Jesse's Python Multi-Paxos implementation"
  :url "https://github.com/ajdavis/python-paxos-jepsen/"
  :license {:name "Public Domain"}
  :main jepsen.paxos
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/data.json "2.4.0"]
                 [jepsen "0.2.3"]]
  :repl-options {:init-ns jepsen.paxos})
