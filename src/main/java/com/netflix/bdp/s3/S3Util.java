/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.bdp.s3;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.netflix.bdp.s3.util.Paths;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

public class S3Util {

  private static final Logger LOG = LoggerFactory.getLogger(S3Util.class);

  public static void revertCommit(S3Client client, PendingUpload commit) {
    client.deleteObject(commit.newDeleteRequest());
  }

  public static void finishCommit(S3Client client, PendingUpload commit) {
    client.completeMultipartUpload(commit.newCompleteRequest());
  }

  public static void abortCommit(S3Client client, PendingUpload commit) {
    client.abortMultipartUpload(commit.newAbortRequest());
  }

  public static PendingUpload multipartUpload(
      S3Client client,
      File localFile,
      String partition,
      String bucket,
      String key,
      int uploadPartSize) {
    Preconditions.checkArgument(localFile.length() > 0, "Cannot upload 0 byte file: " + localFile);

    // Initiate a multipart upload
    CreateMultipartUploadRequest createRequest =
        CreateMultipartUploadRequest.builder().bucket(bucket).key(key).build();

    CreateMultipartUploadResponse createResponse = client.createMultipartUpload(createRequest);
    String uploadId = createResponse.uploadId();

    // Prepare the parts to be uploaded
    Map<Integer, String> partToEtag = Maps.newLinkedHashMap();
    ByteBuffer buffer =
        ByteBuffer.allocate(uploadPartSize); // Set your preferred part size (5 MB in this example)
    int partNumber = 1;
    boolean threw = true;
    try (RandomAccessFile file = new RandomAccessFile(localFile, "r")) {
      long fileSize = file.length();
      long position = 0;

      while (position < fileSize) {
        file.seek(position);
        int bytesRead = file.getChannel().read(buffer);

        buffer.flip();
        UploadPartRequest uploadPartRequest =
            UploadPartRequest.builder()
                .bucket(bucket)
                .key(key)
                .uploadId(uploadId)
                .partNumber(partNumber)
                .contentLength((long) bytesRead)
                .build();

        UploadPartResponse response =
            client.uploadPart(uploadPartRequest, RequestBody.fromByteBuffer(buffer));
        partToEtag.put(partNumber, response.eTag());

        buffer.clear();
        position += bytesRead;
        partNumber++;
      }

      PendingUpload pending = new PendingUpload(partition, bucket, key, uploadId, partToEtag);
      threw = false;
      return pending;

    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    } finally {
      if (threw) {
        try {
          client.abortMultipartUpload(
              AbortMultipartUploadRequest.builder()
                  .bucket(bucket)
                  .key(key)
                  .uploadId(uploadId)
                  .build());
        } catch (SdkException e) {
          LOG.error("Failed to abort multi-part upload", e);
        }
      }
    }
  }

  static List<PendingUpload> readPendingCommits(FileSystem fs, Path pendingCommitsFile)
      throws IOException {
    List<PendingUpload> commits = Lists.newArrayList();

    ObjectInputStream in = new ObjectInputStream(fs.open(pendingCommitsFile));
    boolean threw = true;
    try {
      for (PendingUpload commit : new ObjectIterator<PendingUpload>(in)) {
        commits.add(commit);
      }
      threw = false;
    } finally {
      Closeables.close(in, threw);
    }

    return commits;
  }

  private static class ObjectIterator<T> implements Iterator<T>, Iterable<T> {
    private final ObjectInputStream stream;
    private boolean hasNext;
    private T next;

    public ObjectIterator(ObjectInputStream stream) {
      this.stream = stream;
      readNext();
    }

    @Override
    public Iterator<T> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return hasNext;
    }

    @Override
    public T next() {
      T toReturn = next;
      this.readNext();
      return toReturn;
    }

    @SuppressWarnings("unchecked")
    public void readNext() {
      try {
        this.next = (T) stream.readObject();
        this.hasNext = (next != null);
      } catch (EOFException e) {
        this.hasNext = false;
      } catch (ClassNotFoundException | IOException e) {
        this.hasNext = false;
        Throwables.propagate(e);
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Not implemented");
    }
  }

  /**
   * This class is used to pass information about pending uploads from tasks to the job committer.
   * It is serializable and will instantiate S3 requests.
   */
  public static class PendingUpload implements Serializable {
    private final String partition;
    private final String bucket;
    private final String key;
    private final String uploadId;
    private final SortedMap<Integer, String> parts;

    public PendingUpload(
        String partition, String bucket, String key, String uploadId, Map<Integer, String> etags) {
      this.partition = partition;
      this.bucket = bucket;
      this.key = key;
      this.uploadId = uploadId;
      this.parts = ImmutableSortedMap.copyOf(etags);
    }

    public CompleteMultipartUploadRequest newCompleteRequest() {
      final List<CompletedPart> partsCopy = Lists.newArrayList();

      for (Map.Entry<Integer, String> entry : parts.entrySet()) {
        partsCopy.add(
            CompletedPart.builder().partNumber(entry.getKey()).eTag(entry.getValue()).build());
      }

      return CompleteMultipartUploadRequest.builder()
          .bucket(bucket)
          .key(key)
          .uploadId(uploadId)
          .multipartUpload(CompletedMultipartUpload.builder().parts(partsCopy).build())
          .build();
    }

    public DeleteObjectRequest newDeleteRequest() {
      return DeleteObjectRequest.builder().bucket(bucket).key(key).build();
    }

    public AbortMultipartUploadRequest newAbortRequest() {
      return AbortMultipartUploadRequest.builder()
          .bucket(bucket)
          .key(key)
          .uploadId(uploadId)
          .build();
    }

    public String getBucketName() {
      return bucket;
    }

    public String getKey() {
      return key;
    }

    public String getUploadId() {
      return uploadId;
    }

    public SortedMap<Integer, String> getParts() {
      return parts;
    }

    public String getPartition() {
      return partition;
    }

    public String getLocation() {
      return Paths.getParent(key);
    }
  }
}
