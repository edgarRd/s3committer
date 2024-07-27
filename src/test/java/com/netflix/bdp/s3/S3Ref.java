package com.netflix.bdp.s3;

import java.io.Serializable;

public class S3Ref implements Serializable {
  private final String bucket;
  private final String key;

  S3Ref(String bucket, String key) {
    this.bucket = bucket;
    this.key = key;
  }

  public String bucket() {
    return this.bucket;
  }

  public String key() {
    return this.key;
  }

  public static S3Ref of(String bucket, String key) {
    return new S3Ref(bucket, key);
  }
}
