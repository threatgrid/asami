(ns ^{:doc "Tests the encoding/decoding operations on objects"
      :author "Paula Gearon"}
    asami.durable.object-codec-test
  (:require [asami.durable.codec :as codec]
            [asami.durable.encoder :as encoder :refer [to-bytes encapsulate-id]]
            [asami.durable.decoder :as decoder :refer [read-object unencapsulate-id decode-length-node]]
            [asami.durable.common :refer [Paged refresh! read-byte read-short read-bytes read-bytes-into
                                          FlatStore write-object! get-object force!]]
            [asami.durable.flat-file :refer [paged-file]]
            [clojure.string :as s]
            [clojure.test :refer [deftest is]]
            [clojure.pprint :refer [pprint]])
  (:import [java.io RandomAccessFile File]
           [java.nio ByteBuffer]
           [java.time Instant ZoneOffset]
           [java.util Date Arrays GregorianCalendar TimeZone]
           [java.net URI URL]))

(defn byte-length [bytes]
  (alength bytes))

(defn byte-buffer [byte-array]
  (ByteBuffer/wrap byte-array))

(defrecord TestReader [b]
  Paged
  (refresh! [this])

  (read-byte [this offset]
    (.get b offset))

  (read-short [this offset]
    (.getShort b offset))

  (read-bytes [this offset len]
    (read-bytes-into this offset (byte-array len)))

  (read-bytes-into [this offset bytes]
    (.get (.position (.asReadOnlyBuffer b) offset) bytes 0 (byte-length bytes))
    bytes))

(defn array-equals
  [a b]
  (Arrays/equals a b))

(defn now [] (Date.))

(def data {"layers"
           {:db/ident "layers"
            "frame"
            {:id "frame-data"
             "frame.protocols" "eth:ethertype:ip:tcp",
             "frame.cap_len" "78",
             "frame.marked" "0",
             "frame.offset_shift" "0.000000000",
             "frame.time_delta_displayed" "0.003008000",
             "frame.time_relative" "7.959051000",
             "frame.time_delta" "0.003008000",
             "frame.time_epoch" "1612222673.161785000",
             "frame.time" "Feb  1, 2021 18:37:53.161785000 EST",
             "frame.encap_type" "1",
             "frame.len" "78",
             "frame.number" "771",
             "frame.ignored" "0"},
            "eth"
            {:id "eth-data"
             "eth.dst" "22:4e:7f:74:55:8d",
             "eth.dst_tree"
             {"eth.dst.oui" "2248319",
              "eth.addr" "22:4e:7f:74:55:8d",
              "eth.dst_resolved" "22:4e:7f:74:55:8d",
              "eth.dst.ig" "0",
              "eth.ig" "0",
              "eth.lg" "1",
              "eth.addr_resolved" "22:4e:7f:74:55:8d",
              "eth.dst.lg" "1",
              "eth.addr.oui" "2248319"},
             "eth.src" "46:eb:d7:d5:2b:c8",
             "eth.src_tree"
             {"eth.addr" "46:eb:d7:d5:2b:c8",
              "eth.ig" "0",
              "eth.lg" "1",
              "eth.src.oui" "4647895",
              "eth.addr_resolved" "46:eb:d7:d5:2b:c8",
              "eth.src.lg" "1",
              "eth.src.ig" "0",
              "eth.addr.oui" "4647895",
              "eth.src_resolved" "46:eb:d7:d5:2b:c8"},
             "eth.type" "0x00000800"},
            "ip"
            {:id "ip-data"
             "ip.checksum.status" "2",
             "ip.dst_host" "199.232.37.87",
             "ip.host" "199.232.37.87",
             "ip.dsfield" "0x00000000",
             "ip.version" "4",
             "ip.len" "64",
             "ip.src" "192.168.1.122",
             "ip.addr" "199.232.37.87",
             "ip.frag_offset" "0",
             "ip.dsfield_tree" {"ip.dsfield.dscp" "0", "ip.dsfield.ecn" "0"},
             "ip.ttl" "64",
             "ip.checksum" "0x00008b56",
             "ip.id" "0x00000000",
             "ip.proto" "6",
             "ip.flags_tree"
             {"ip.flags.rb" "0", "ip.flags.df" "1", "ip.flags.mf" "0"},
             "ip.hdr_len" "20",
             "ip.dst" "199.232.37.87",
             "ip.src_host" "192.168.1.122",
             "ip.flags" "0x00000040"},
            "tcp"
            {:id "tcp-data"
             "tcp.srcport" "57934",
             "tcp.seq" "518",
             "tcp.options_tree"
             {"tcp.options.nop" "01",
              "tcp.options.nop_tree" {"tcp.option_kind" "1"},
              "tcp.options.timestamp" "08:0a:2a:c2:dd:68:4b:8d:11:f5",
              "tcp.options.timestamp_tree"
              {"tcp.option_kind" "8",
               "tcp.option_len" "10",
               "tcp.options.timestamp.tsval" "717413736",
               "tcp.options.timestamp.tsecr" "1267536373"},
              "tcp.options.sack" "05:0a:1d:4e:a3:7c:1d:4e:ae:4c",
              "tcp.options.sack_tree"
              {"tcp.option_kind" "5",
               "tcp.option_len" "10",
               "tcp.options.sack_le" "1385",
               "tcp.options.sack_re" "4153",
               "tcp.options.sack.count" "1",
               "tcp.options.sack.dsack_le" "1385",
               "tcp.options.sack.dsack_re" "4153",
               "D-SACK Sequence"
               {"_ws.expert"
                {:db/ident :dsack
                 "tcp.options.sack.dsack" "",
                 "_ws.expert.message" "D-SACK Sequence",
                 "_ws.expert.severity" "6291456",
                 "_ws.expert.group" "33554432"}}}},
             "tcp.window_size" "131456",
             "tcp.dstport" "443",
             "tcp.urgent_pointer" "0",
             "tcp.nxtseq" "518",
             "tcp.ack_raw" "491691540",
             "tcp.options"
             "01:01:08:0a:2a:c2:dd:68:4b:8d:11:f5:01:01:05:0a:1d:4e:a3:7c:1d:4e:ae:4c",
             "tcp.stream" "35",
             "tcp.hdr_len" "44",
             "tcp.seq_raw" "3253069772",
             "tcp.checksum" "0x00004606",
             "tcp.port" "443",
             "tcp.ack" "1",
             "Timestamps"
             {"tcp.time_relative" "0.075895000", "tcp.time_delta" "0.003835000"},
             "tcp.window_size_scalefactor" "32",
             "tcp.checksum.status" "2",
             "tcp.flags" "0x00000010",
             "tcp.window_size_value" "4108",
             "tcp.len" "0",
             "tcp.flags_tree"
             {"tcp.flags.ecn" "0",
              "tcp.flags.res" "0",
              "tcp.flags.cwr" "0",
              "tcp.flags.syn" "0",
              "tcp.flags.urg" "0",
              "tcp.flags.fin" "0",
              "tcp.flags.push" "0",
              "tcp.flags.str" "·······A····",
              "tcp.flags.reset" "0",
              "tcp.flags.ns" "0",
              "tcp.flags.ack" "1"},
             "tcp.analysis"
             {"tcp.analysis.initial_rtt" "0.042200000",
              "tcp.analysis.flags" {"tcp.analysis.duplicate_ack" ""},
              "tcp.analysis.duplicate_ack_num" "2",
              "tcp.analysis.duplicate_ack_frame" "754",
              "tcp.analysis.duplicate_ack_frame_tree"
              {"_ws.expert"
               {"tcp.analysis.duplicate_ack" "",
                "_ws.expert.message" "Duplicate ACK (#2)",
                "_ws.expert.severity" "4194304",
                "_ws.expert.group" "33554432"}}}}}})

(deftest test-map-codecs
  (let [buffer-size 10240
        bb (byte-buffer (byte-array buffer-size))
        rdr (->TestReader bb)]
    (let [[header body offsets] (to-bytes data)
          header-size (byte-length header)
          body-size (byte-length body)]
      (if (>= (+ header-size body-size) buffer-size)
        (throw (ex-info "Buffer too small" {:header (alength header) :body (alength body)})))
      (.position bb 0)
      (.put bb header 0 header-size)
      (.put bb body 0 body-size)
      ;;(pprint offsets)
      )))


