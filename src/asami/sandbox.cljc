(ns ^{:doc "Provides data on which functions are allowed to be accessed in sandbox conditions"
      :author "Paula Gearon"}
    asami.sandbox)

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
    'with-redefs-fn})

(def allowed-fns
  #{'* '*' '+ '+' '- '-' '/ '< '<= '= '== '> '>= 'aget 'alength 'any? 'apply 'array-map 'aset 'aset-boolean
    'aset-byte 'aset-char 'aset-double 'aset-float 'aset-int 'aset-long 'aset-short 'assoc
    'assoc! 'assoc-in 'associative? 'bases 'bean 'bigdec 'bigint 'biginteger 'bit-and 'bit-and-not
    'bit-clear 'bit-flip 'bit-not 'bit-or 'bit-set 'bit-shift-left 'bit-shift-right 'bit-test
    'bit-xor 'boolean 'boolean-array 'boolean? 'booleans 'bounded-count 'butlast 'byte
    'byte-array 'bytes 'bytes? 'cat 'char 'char-array 'char-escape-string 'char-name-string
    'char? 'chars 'class 'class? 'clojure-version 'coll? 'comp 'comparator 'compare
    'compare-and-set! 'complement 'completing 'concat 'conj 'conj! 'cons 'constantly
    'construct-proxy 'contains? 'count 'counted? 'create-struct 'cycle 'dec 'dec' 'decimal?
    'dedupe 'default-data-readers 'delay? 'deliver 'denominator 'deref 'derive 'descendants 'disj
    'disj! 'dissoc 'dissoc! 'distinct 'distinct? 'doall 'dorun 'double 'double-array 'double?
    'doubles 'drop 'drop-last 'drop-while 'eduction 'empty 'empty? 'ensure-reduced
    'enumeration-seq 'even? 'every-pred 'every? 'extends? 'false? 'ffirst 'filter 'filterv 'find
    'find-keyword 'first 'flatten 'float 'float-array 'float? 'floats 'fn? 'fnext 'fnil 'force
    'format 'frequencies 'gensym 'get 'get-in 'group-by 'halt-when 'hash 'hash-map
    'hash-ordered-coll 'hash-set 'hash-unordered-coll 'ident? 'identical? 'identity 'ifn? 'inc
    'inc' 'indexed? 'inst-ms 'inst-ms* 'inst? 'instance? 'int 'int-array 'int? 'integer?
    'interleave 'interpose 'into 'into-array 'ints 'isa? 'iterate 'iterator-seq 'juxt 'keep
    'keep-indexed 'key 'keys 'keyword 'keyword? 'last 'list 'list* 'list? 'long 'long-array
    'longs 'make-array 'make-hierarchy 'map 'map-entry? 'map-indexed 'map? 'mapcat 'mapv 'max
    'max-key 'merge 'merge-with 'meta 'methods 'min 'min-key 'mix-collection-hash 'mod 'name
    'namespace 'nat-int? 'neg-int? 'neg? 'next 'nfirst 'nil? 'nnext 'not 'not-any? 'not-empty
    'not-every? 'not= 'nth 'nthnext 'nthrest 'num 'number? 'numerator 'object-array 'odd? 'parents
    'partial 'partition 'partition-all 'partition-by 'peek 'pop 'pos-int? 'pos? 'print-str
    'println-str 'prn-str 'qualified-ident? 'qualified-keyword? 'qualified-symbol? 'quot 'rand
    'rand-int 'rand-nth 'random-sample 'range 'ratio? 'rational? 'rationalize 're-find 're-groups
    're-matcher 're-matches 're-pattern 're-seq 'reader-conditional 'reader-conditional?
    'realized? 'record? 'reduce 'reduce-kv 'reduced 'reduced? 'reductions 'rem 'remove 'repeat
    'repeatedly 'replace 'rest 'reverse 'reversible? 'rseq 'rsubseq 'run! 'satisfies? 'second
    'select-keys 'seq 'seq? 'seqable? 'sequence 'sequential? 'set 'set? 'short 'short-array 'shorts
    'shuffle 'simple-ident? 'simple-keyword? 'simple-symbol? 'slurp 'some 'some-fn 'some? 'sort
    'sort-by 'sorted-map 'sorted-map-by 'sorted-set 'sorted-set-by 'sorted? 'special-symbol?
    'split-at 'split-with 'str 'string? 'struct 'struct-map 'subs 'subseq 'subvec 'supers 'symbol
    'symbol? 'tagged-literal 'tagged-literal? 'take 'take-last 'take-nth 'take-while 'test
    'to-array 'to-array-2d 'transduce 'tree-seq 'true? 'type 'unchecked-add 'unchecked-add-int
    'unchecked-byte 'unchecked-char 'unchecked-dec 'unchecked-dec-int 'unchecked-divide-int
    'unchecked-double 'unchecked-float 'unchecked-inc 'unchecked-inc-int 'unchecked-int
    'unchecked-long 'unchecked-multiply 'unchecked-multiply-int 'unchecked-negate
    'unchecked-negate-int 'unchecked-remainder-int 'unchecked-short 'unchecked-subtract
    'unchecked-subtract-int 'underive 'unquote 'unquote-splicing 'unreduced
    'unsigned-bit-shift-right 'update 'update-in 'update-proxy 'uri? 'uuid? 'val 'vals 'vec
    'vector 'vector-of 'vector? 'with-meta 'xml-seq 'zero? 'zipmap})
