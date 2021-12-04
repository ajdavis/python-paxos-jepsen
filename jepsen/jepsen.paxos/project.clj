(defproject jepsen.paxos "0.1.0-SNAPSHOT"
  :description "Test Jesse's Python Multi-Paxos implementation"
  :url "https://github.com/ajdavis/python-paxos-jepsen/"
  :license {:name "Public Domain"}
  :main jepsen.paxos
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [jepsen "0.2.1-SNAPSHOT"]]
  :repl-options {:init-ns jepsen.paxos})