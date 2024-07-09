package xtdb.aws.s3

import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest

interface S3Configurator {
    fun configureGet(builder: GetObjectRequest.Builder) = builder
    fun configureHead(builder: HeadObjectRequest.Builder) = builder
    fun configurePut(builder: PutObjectRequest.Builder) = builder
    fun makeClient(): S3AsyncClient = S3AsyncClient.create()
}

data object DefaultS3Configurator: S3Configurator
