package core2.s3;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public interface S3Configurator {
    default GetObjectRequest.Builder configureGet(GetObjectRequest.Builder builder) {
        return builder;
    }

    default HeadObjectRequest.Builder configureHead(HeadObjectRequest.Builder builder) {
        return builder;
    }

    default PutObjectRequest.Builder configurePut(PutObjectRequest.Builder builder) {
        return builder;
    }

    default S3AsyncClient makeClient() {
        return S3AsyncClient.create();
    }
}
