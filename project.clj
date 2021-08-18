(defproject org.clojars.quoll/asami "2.1.2"
  :description "An in memory graph store for Clojure and ClojureScript"
  :url "http://github.com/threatgrid/asami"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [prismatic/schema "1.1.12"] 
                 [org.clojure/core.cache "1.0.207"]
                 [org.clojars.quoll/zuko "0.6.5"]
                 [org.clojars.quoll/qtest "0.1.1"]
                 [org.clojure/data.priority-map "1.0.0"]
                 [tailrecursion/cljs-priority-map "1.2.1"]]
  :plugins [[lein-cljsbuild "1.1.8"]]
  :profiles {:dev {:dependencies [[org.clojure/clojurescript "1.10.866"]]}
             :uberjar {:aot [asami.peer]}
             :native {:plugins [[lein-shell "0.5.0"]]
                      :source-paths ["src" "src-native"]
                      :aot :all
                      :main asami.main
                      :dependencies [[cheshire "5.10.0"]]
                      :aliases {"native" ["shell"
                                          "native-image" "--report-unsupported-elements-at-runtime"
                                          "--initialize-at-build-time" "--no-server"
                                          "-jar" "./target/${:uberjar-name:-${:name}-${:version}-standalone.jar}"
                                          "-H:Name=./target/${:name}"
                                          "-H:TraceClassInitialization=\"java.io.FilePermission\""]}}
             :test-native {:source-paths ["src" "src-native" "test-native"]
                           :test-paths ["test-native"]
                           :dependencies [[cheshire "5.10.0"]]}}
  :cljsbuild {:builds {:dev {:source-paths ["src"]
                             :compiler {:output-to "out/asami/core.js"
                                        :optimizations :simple
                                        :pretty-print true}}
                       :test {:source-paths ["src" "test"]
                              :compiler {:output-to "out/asami/test_memory.js"
                                         :optimizations :simple
                                         :pretty-print true}}}
              :test-commands {"unit" ["node" "out/asami/test_memory.js"]}})
