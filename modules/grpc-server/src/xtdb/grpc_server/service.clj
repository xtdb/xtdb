(ns xtdb.grpc-server.service
  #_(:gen-class
   :name xtdb-grpc-server.service.GreeterServiceImpl
   :extends
   io.grpc.examples.helloworld.GreeterGrpc$GreeterImplBase)
  #_(:import
   [io.grpc.stub StreamObserver]
   [io.grpc.examples.helloworld
    HelloReply]))


#_(defn -sayHello [this req res]
  (let [name (.getName req)]
    (doto res
      (.onNext (-> (HelloReply/newBuilder)
                   (.setMessage (str "Hello, " name))
                   (.build)))
      (.onCompleted))))