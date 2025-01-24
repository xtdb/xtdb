package xtdb.aws.s3

import software.amazon.awssdk.services.s3.S3AsyncClientBuilder
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest

interface S3Configurator {
    fun configureGet(builder: GetObjectRequest.Builder) = Unit
    fun configureHead(builder: HeadObjectRequest.Builder) = Unit
    fun configurePut(builder: PutObjectRequest.Builder) = Unit
    fun configureClient(builder: S3AsyncClientBuilder) = Unit

    data object Default: S3Configurator
}
