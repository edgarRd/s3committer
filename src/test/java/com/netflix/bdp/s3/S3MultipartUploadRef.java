package com.netflix.bdp.s3;

public final class S3MultipartUploadRef extends S3Ref {
  private final String uploadId;

  private S3MultipartUploadRef(String bucket, String key, String uploadId) {
    super(bucket, key);
    this.uploadId = uploadId;
  }

  public String bucket() {
    return super.bucket();
  }

  public String key() {
    return super.key();
  }

  public String uploadId() {
    return this.uploadId;
  }

  public static S3MultipartUploadRef of(String bucket, String key, String uploadId) {
    return new S3MultipartUploadRef(bucket, key, uploadId);
  }
}
