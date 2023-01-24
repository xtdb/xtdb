package xtdb.s3;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.transfer.s3.model.UploadDirectoryRequest;
import software.amazon.awssdk.transfer.s3.model.DownloadDirectoryRequest;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

public interface S3Configurator {
    default PutObjectRequest.Builder configurePut(PutObjectRequest.Builder builder) {
        return builder;
    }

    default GetObjectRequest.Builder configureGet(GetObjectRequest.Builder builder) {
        return builder;
    }

    default UploadDirectoryRequest.Builder configureUploadDirectory(UploadDirectoryRequest.Builder builder) {
        return builder;
    }

    default DownloadDirectoryRequest.Builder configureDownloadDirectory(DownloadDirectoryRequest.Builder builder) {
        return builder;
    }

    default S3AsyncClient makeClient() {
        return S3AsyncClient.create();
    }

    default byte[] freeze(Object doc) {
        return NippySerde.freeze(doc);
    }

    default Object thaw(byte[] bytes) {
        return NippySerde.thaw(bytes);
    }
}

class NippySerde {
    private static final IFn requiringResolve = Clojure.var("clojure.core/requiring-resolve");
    private static final IFn fastFreeze = (IFn) requiringResolve.invoke(Clojure.read("juxt.clojars-mirrors.nippy.v3v1v1.taoensso.nippy/fast-freeze"));
    private static final IFn fastThaw = (IFn) requiringResolve.invoke(Clojure.read("juxt.clojars-mirrors.nippy.v3v1v1.taoensso.nippy/fast-thaw"));

    static byte[] freeze(Object doc) {
        return (byte[]) fastFreeze.invoke(doc);
    }

    static Object thaw(byte[] bytes) {
        return fastThaw.invoke(bytes);
    }
}
