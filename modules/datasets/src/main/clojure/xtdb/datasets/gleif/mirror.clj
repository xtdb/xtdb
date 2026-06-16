(ns xtdb.datasets.gleif.mirror
  "Produces the GLEIF dataset for S3: GLEIF LEI-CDF 3.1 JSON → idiomatic Clojure
   records → transit, fetched and transformed in one Clojure pass.

   Run occasionally, from the REPL (see the comment at the foot of this ns), like
   tsbs's generator ritual: `mirror!` fetches the GLEIF API and writes curated
   transit.json.gz into an out-dir; scripts/upload-gleif.sh then syncs that dir to
   s3://xtdb-datasets/gleif/. Consumers sync it *down* via download-dataset.sh and
   load with xtdb.datasets.gleif — no GLEIF-API or JSON-decoding on the load path.

   GLEIF JSON wraps every scalar as {\"$\" v}, sometimes with sibling @-attributes,
   and emits a single repeating element as a bare object / several as an array.
   All of that GLEIF-specific decoding lives here; the records on the wire are
   plain Clojure (kebab keywords, java.time.Instant, nested maps/vectors), with
   events still a flat :events list — record->docs does the XT-table shaping."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [cognitect.transit :as transit]
            [hato.client :as http]
            [jsonista.core :as json]
            [xtdb.serde :as serde])
  (:import [com.fasterxml.jackson.core JsonParser JsonToken]
           [java.time Instant LocalDate OffsetDateTime ZoneOffset]
           [java.time.format DateTimeParseException]
           [java.util.zip GZIPInputStream GZIPOutputStream ZipInputStream]))

;; Curation is a predicate over the raw GLEIF record. The demo keeps GB-jurisdiction
;; entities — a representative, bounded slice (~1-2% of the register, tens of
;; thousands of entities) with real event history, rather than a hand-picked few.
(defn gb-jurisdiction?
  "Keep entities whose legal jurisdiction is Great Britain."
  [record]
  (= "GB" (get-in record ["Entity" "LegalJurisdiction" "$"])))

(defn- parse-instant ^Instant [s]
  ;; GLEIF datetimes carry a zone offset (…Z or …+01:00); OffsetDateTime handles
  ;; both, Instant/parse rejects non-UTC. Some values are date-only. A genuinely
  ;; unparseable value warns and yields nil rather than aborting the mirror.
  (when-not (str/blank? s)
    (try
      (.toInstant (OffsetDateTime/parse s))
      (catch DateTimeParseException _
        (try
          (-> (LocalDate/parse s) (.atStartOfDay ZoneOffset/UTC) .toInstant)
          (catch DateTimeParseException _
            (log/warn "unparseable GLEIF date, skipping value:" (pr-str s))
            nil))))))

(defn- $val
  "Unwrap a GLEIF scalar node {\"$\" v} (ignoring @-attributes). nil-safe."
  [node]
  (when (map? node) (get node "$")))

(defn- ->kw
  "GLEIF UPPER_SNAKE enum string → kebab keyword (CHANGE_LEGAL_NAME → :change-legal-name)."
  [s]
  (when-not (str/blank? s)
    (keyword (str/lower-case (str/replace s "_" "-")))))

(defn- ->seq
  "A single repeating element is a bare object, several an array — normalise to a seq."
  [node]
  (cond (nil? node) nil
        (sequential? node) node
        :else [node]))

(defn- $inst [node] (parse-instant ($val node)))

(defn- address [addr]
  (when addr
    {:first-line ($val (get addr "FirstAddressLine"))
     :additional-lines (mapv $val (->seq (get addr "AdditionalAddressLine")))
     :city ($val (get addr "City"))
     :region ($val (get addr "Region"))
     :country ($val (get addr "Country"))
     :postal-code ($val (get addr "PostalCode"))}))

(defn- other-name [nm]
  {:type (->kw (get nm "@type"))
   :name ($val nm)})

(defn- event [ev]
  {:type (->kw ($val (get ev "LegalEntityEventType")))
   :effective-date ($inst (get ev "LegalEntityEventEffectiveDate"))
   :recorded-date ($inst (get ev "LegalEntityEventRecordedDate"))
   :status (->kw (get ev "@event_status"))
   :group-type (->kw (get ev "@group_type"))
   :validation-documents (->kw ($val (get ev "ValidationDocuments")))})

(defn record->idiomatic
  "A raw GLEIF lei2 JSON record → an idiomatic Clojure record. Pure decode — no
   filtering (the caller applies that). Events stay a flat :events list;
   record->docs does the XT-table split."
  [record]
  (let [entity (get record "Entity")
        reg (get record "Registration")]   ; record-level sibling of Entity
    {:lei ($val (get record "LEI"))
     :legal-name ($val (get entity "LegalName"))
     :legal-address (address (get entity "LegalAddress"))
     :hq-address (address (get entity "HeadquartersAddress"))
     :jurisdiction ($val (get entity "LegalJurisdiction"))
     :legal-form-code ($val (get-in entity ["LegalForm" "EntityLegalFormCode"]))
     :entity-category (->kw ($val (get entity "EntityCategory")))
     :entity-status (->kw ($val (get entity "EntityStatus")))
     :entity-creation-date ($inst (get entity "EntityCreationDate"))
     :registration {:status (->kw ($val (get reg "RegistrationStatus")))
                    :initial-date ($inst (get reg "InitialRegistrationDate"))
                    :last-update ($inst (get reg "LastUpdateDate"))
                    :next-renewal ($inst (get reg "NextRenewalDate"))
                    :managing-lou ($val (get reg "ManagingLOU"))}
     :other-names (mapv other-name (->seq (get-in entity ["OtherEntityNames" "OtherEntityName"])))
     :events (mapv event (->seq (get-in entity ["LegalEntityEvents" "LegalEntityEvent"])))}))

;;; --- fetch: GLEIF API → raw lei2 JSON (run from the REPL comment below) -------

(def ^:private publishes-url
  "https://goldencopy.gleif.org/api/v2/golden-copies/publishes")

(defn- get-json [url opts]
  (-> (http/get url (merge {:as :stream} opts)) :body json/read-value))

(defn- lei2-json-stream
  "Fetch a GLEIF .json.zip URL and return an InputStream over the single JSON
   entry inside the zip — streamed, not slurped (the anchor full file is ~400MB).
   The caller owns + closes it."
  ^java.io.InputStream [url]
  (let [zin (ZipInputStream. (-> (http/get url {:as :stream}) :body))]
    (.getNextEntry zin)          ; the publish files hold exactly one entry
    zin))

(defn- advance-to-records!
  "Drive `p` to the start of the top-level \"records\" array's contents."
  [^JsonParser p]
  (.nextToken p)                                 ; START_OBJECT
  (loop []
    (when-not (= "records" (.nextFieldName p))
      (.skipChildren p)
      (recur)))
  (.nextToken p))                                ; START_ARRAY

(defn- mirror-stream!
  "Stream the lei2 JSON on `in` to a transit.json.gz at out-file, one record at a
   time so memory stays flat regardless of file size: keep those matching `keep?`,
   decode each to idiomatic. Owns only `out`; the caller owns `in`."
  [in out-file keep?]
  (io/make-parents out-file)
  (with-open [out (-> (io/output-stream out-file) GZIPOutputStream.)
              p (.createParser json/default-object-mapper ^java.io.InputStream in)]
    (advance-to-records! p)
    (let [wtr (transit/writer out :json {:handlers serde/transit-write-handler-map})]
      (loop [seen 0, kept 0]
        (if (= JsonToken/START_OBJECT (.nextToken p))
          ;; Object → the mapper's Clojure module yields a persistent map (string
          ;; keys), so $val/->seq's map?/sequential? predicates hold.
          (let [rec (.readValueAs p Object)]          ; parses just this record's subtree
            (if (keep? rec)
              (do (transit/write wtr (record->idiomatic rec))
                  (.write out (int \newline))        ; one record per line — greppable/streamable
                  (recur (inc seen) (inc kept)))
              (recur (inc seen) kept)))
          (do (log/info "mirrored" kept "/" seen "records →" (.getName (io/file out-file)))
              kept))))))

(defn- mirror-url!
  "Fetch a GLEIF lei2 .json.zip URL and mirror it to out-file. Owns the fetched
   stream (open + close)."
  [url out-file keep?]
  (log/info "fetching" url)
  (with-open [in (lei2-json-stream url)]
    (mirror-stream! in out-file keep?)))

(defn mirror!
  "Fetch + decode + curate GLEIF straight to transit under out-dir: an anchor
   full-file snapshot at `anchor-date`, plus every IntraDay lei2 delta from there
   to now. Run from the REPL (see the comment below); upload-gleif.sh then syncs
   out-dir → s3://xtdb-datasets/gleif/. `keep?` is a predicate over the raw record
   — defaults to GB-jurisdiction entities; pass (constantly true) for the whole register.

   Logs progress throughout — it makes ~110 HTTP round-trips to find the anchor
   and a ~400MB fetch, so the per-page/per-file lines are how you see it's alive."
  [out-dir {:keys [anchor-date keep?] :or {keep? gb-jurisdiction?}}]
  (log/info "mirroring GLEIF to" (str out-dir) "(anchor" anchor-date ")")
  ;; Single pass, newest-first: grab each publish's IntraDay delta on the way down,
  ;; and when we reach the publish at/before the anchor, grab its full file and stop.
  ;; (Deltas come out newest-first; the loader sorts by date before replaying.)
  ;; key files by the full publish timestamp — GLEIF publishes ~3x/day, so date
  ;; alone would collide and we'd keep only one per day. The anchor gets the same
  ;; treatment so the loader parses its system-time from the filename uniformly.
  (let [->stamp (fn [publish-date]                ; "2026-06-15 16:00:00" → "20260615_160000"
                  (-> publish-date (str/replace #"[-: ]" "") (str/replace #"^(\d{8})" "$1_")))
        mirror-delta! (fn [n pub]                ; mirror one publish's IntraDay delta, if present
                        (let [publish-date (get pub "publish_date")
                              day (subs publish-date 0 10)
                              url (get-in pub ["lei2" "delta_files" "IntraDay" "json" "url"])]
                          (if (and url (> (compare day anchor-date) 0))
                            (do (mirror-url! url
                                             (io/file out-dir (str "deltas/lei2_" (->stamp publish-date) ".transit.json.gz"))
                                             keep?)
                                (inc n))
                            n)))]
    (log/info "walking publishes newest-first (deltas as we go; ~110 pages to the anchor)...")
    (loop [page 1, n-deltas 0]
      (let [data (get (get-json publishes-url {:query-params {:page page :per_page 10}}) "data")]
        (when (zero? (mod page 10)) (log/info "  ...page" page "—" n-deltas "deltas so far"))
        (if-let [anchor-pub (some (fn [p] (when (<= (compare (subs (get p "publish_date") 0 10) anchor-date) 0) p))
                                  data)]
          ;; this page reaches the anchor: mirror the deltas above it, then the anchor full file, done.
          (let [n-deltas (reduce mirror-delta! n-deltas data)]
            (log/info "anchor publish:" (subs (get anchor-pub "publish_date") 0 10) "(page" (str page ")"))
            (mirror-url! (get-in anchor-pub ["lei2" "full_file" "json" "url"])
                         (io/file out-dir (str "anchor/lei2_" (->stamp (get anchor-pub "publish_date")) ".transit.json.gz"))
                         keep?)
            (log/info "done —" n-deltas "delta files +1 anchor mirrored to" (str out-dir)))
          ;; no anchor on this page yet: mirror every delta here and page on.
          (when (seq data)
            (recur (inc page) (reduce mirror-delta! n-deltas data))))))))

(comment
  ;; produce the demo dataset (GB-jurisdiction entities by default), then sync to S3:
  (mirror! "src/test/resources/data/gleif" {:anchor-date "2025-06-15"})
  ;; → scripts/upload-gleif.sh   (aws s3 sync that dir → s3://xtdb-datasets/gleif/)

  ;; whole register (big): 
  (mirror! "src/test/resources/data/gleif" {:anchor-date "2025-06-15" :keep? (constantly true)})
  )

