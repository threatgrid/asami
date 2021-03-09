(ns ^{:doc "Provides data on which functions are allowed to be accessed in sandbox conditions"
      :author "Paula Gearon"}
    asami.sandbox)

;; This is not required, but is useful to keep as a list.
;; Removing from CLJS to avoid referencing unused functions
#?(:clj
(def disallowed-fns
  #{'-> '->> '.. 'amap 'and 'areduce 'as-> 'assert 'binding 'bound-fn 'case 'comment 'cond
    'cond-> 'cond->> 'condp 'declare 'definline 'definterface 'defmacro 'defmethod
    'defmulti 'defn 'defn- 'defonce 'defprotocol 'defrecord 'defstruct 'deftype 'delay
    'doseq 'dosync 'dotimes 'doto 'extend-protocol 'extend-type 'fn 'for 'future 'gen-class
    'gen-interface 'if-let 'if-not 'if-some 'import 'io! 'lazy-cat 'lazy-seq 'let 'letfn
    'locking 'loop 'memfn 'ns 'or 'proxy 'proxy-super 'pvalues 'refer-clojure 'reify 'some->
    'some->> 'sync 'time 'vswap! 'when 'when-first 'when-let 'when-not 'when-some 'while
    'with-bindings 'with-in-str 'with-loading-context 'with-local-vars 'with-open
    'with-out-str 'with-precision 'with-redefs

    '*1 '*2 '*3 '*agent* '*allow-unresolved-vars* '*assert* '*clojure-version*
    '*command-line-args* '*compiler-options* '*compile-files* '*compile-path* '*e
    '*err* '*file* '*flush-on-newline* '*fn-loader* '*in* '*default-data-reader-fn*
    '*math-context* '*data-readers* '*ns* '*out* '*print-dup* '*print-length*
    '*print-level* '*print-meta* '*print-namespace-maps* '*print-readably*
    '*read-eval* '*reader-resolver* '*source-path* '*suppress-read* '*unchecked-math*
    '*use-context-classloader* '*verbose-defrecords* '*warn-on-reflection*

    '->ArrayChunk '->Eduction '->Vec '->VecNode '->VecSeq '-cache-protocol-fn
    '-reset-methods 'EMPTY-NODE 'Inst 'PrintWriter-on 'StackTraceElement->vec
    'Throwable->map 'accessor 'aclone 'add-classpath 'add-tap 'add-watch 'agent
    'agent-error 'agent-errors 'alias 'all-ns 'alter 'alter-meta! 'alter-var-root
    'ancestors 'atom 'await 'await-for 'await1 'bound-fn* 'bound? 'cast 'chunk
    'chunk-append 'chunk-buffer 'chunk-cons 'chunk-first 'chunk-next 'chunk-rest
    'chunked-seq? 'clear-agent-errors 'commute 'compile 'create-ns 'destructure
    'ensure 'error-handler 'error-mode 'eval 'ex-cause 'ex-data 'ex-info 'ex-message
    'extend 'extenders 'file-seq 'find-ns 'find-protocol-impl 'find-protocol-method
    'find-var 'flush 'future-call 'future-cancel 'future-cancelled? 'future-done?
    'future? 'get-method 'get-proxy-class 'get-thread-bindings 'get-validator
    'hash-combine 'in-ns 'init-proxy 'intern 'line-seq 'load 'load-file 'load-reader
    'load-string 'loaded-libs 'macroexpand 'macroexpand-1 'memoize 'method-sig
    'munge 'namespace-munge 'newline 'ns-aliases 'ns-imports 'ns-interns 'ns-map
    'ns-name 'ns-publics 'ns-refers 'ns-resolve 'ns-unalias 'ns-unmap 'pcalls
    'persistent! 'pmap 'pop! 'pop-thread-bindings 'pr 'pr-str 'prefer-method
    'prefers 'primitives-classnames 'print 'print-ctor 'print-dup 'print-method
    'print-simple 'printf 'println 'prn 'promise 'proxy-call-with-super 'proxy-mappings
    'proxy-name 'push-thread-bindings 'read 'read+string 'read-line 'read-string 'ref
    'ref-history-count 'ref-max-history 'ref-min-history 'ref-set 'refer
    'release-pending-sends 'remove-all-methods 'remove-method 'remove-ns 'remove-tap
    'remove-watch 'replicate 'require 'requiring-resolve 'reset! 'reset-meta!
    'reset-vals! 'resolve 'restart-agent 'resultset-seq 'send 'send-off 'send-via
    'seque 'set-agent-send-executor! 'set-agent-send-off-executor!
    'set-error-handler! 'set-error-mode! 'set-validator! 'shutdown-agents 'spit
    'swap! 'swap-vals! 'tap> 'the-ns 'thread-bound? 'trampoline 'transient 'use 'var-get
    'var-set 'var? 'vary-meta 'volatile! 'volatile? 'vreset! 'with-bindings*
    'with-redefs-fn}))
