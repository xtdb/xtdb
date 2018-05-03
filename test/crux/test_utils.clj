(ns crux.test-utils
  (:import [java.io File]
           [java.nio.file Files FileVisitResult SimpleFileVisitor]
           [java.nio.file.attribute FileAttribute]
           [java.net ServerSocket]))

(defn free-port ^long []
  (with-open [s (ServerSocket. 0)]
    (.getLocalPort s)))

(defn create-tmpdir ^java.io.File [dir-name]
  (.toFile (Files/createTempDirectory dir-name (make-array FileAttribute 0))))

(def file-deletion-visitor
  (proxy [SimpleFileVisitor] []
    (visitFile [file _]
      (Files/delete file)
      FileVisitResult/CONTINUE)

    (postVisitDirectory [dir _]
      (Files/delete dir)
      FileVisitResult/CONTINUE)))

(defn delete-dir [^File dir]
  (Files/walkFileTree (.toPath dir) file-deletion-visitor))
