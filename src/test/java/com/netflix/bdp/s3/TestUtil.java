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

import static com.netflix.bdp.s3.S3Committer.UPLOAD_UUID;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

public class TestUtil {
  /** Provides setup/teardown of a MiniDFSCluster for tests that need one. */
  public static class MiniDFSTest {
    private static Configuration conf = null;
    private static MiniDFSCluster cluster = null;
    private static FileSystem dfs = null;
    private static FileSystem lfs = null;

    protected static Configuration getConfiguration() {
      return conf;
    }

    protected static FileSystem getDFS() {
      return dfs;
    }

    protected static FileSystem getFS() {
      return lfs;
    }

    @BeforeClass
    @SuppressWarnings("deprecation")
    public static void setupFS() throws IOException {
      if (cluster == null) {
        Configuration c = new Configuration();
        c.setBoolean("dfs.webhdfs.enabled", true);
        // if this fails with "The directory is already locked" set umask to 0022
        cluster = new MiniDFSCluster(c, 1, true, null);
        // cluster = new MiniDFSCluster.Builder(new Configuration()).build();
        dfs = cluster.getFileSystem();
        conf = new Configuration(dfs.getConf());
        lfs = FileSystem.getLocal(conf);
      }
    }

    @AfterClass
    public static void teardownFS() throws IOException {
      dfs = null;
      lfs = null;
      conf = null;
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
    }
  }

  public abstract static class JobCommitterTest<C extends OutputCommitter> {
    private static final JobID JOB_ID = new JobID("job", 1);
    private static final TaskID TASK_ID = new TaskID(JOB_ID, TaskType.JOB_CLEANUP, 1);
    private static final TaskAttemptID TASK_ATTEMP_ID = new TaskAttemptID(TASK_ID, 1);
    private static final Configuration CONF = new Configuration();

    protected static final String OUTPUT_PREFIX = "output/path";
    protected static final Path OUTPUT_PATH =
        new Path("s3://" + MockS3FileSystem.BUCKET + "/" + OUTPUT_PREFIX);

    // created in BeforeClass
    private FileSystem mockFS = null;
    private TaskAttemptContext job = null;

    // created in Before
    private TestUtil.ClientResults results = null;
    private TestUtil.ClientErrors errors = null;
    private S3Client mockClient = null;

    @BeforeClass
    public static void setupMockS3FileSystem() {
      CONF.set("fs.s3.impl", MockS3FileSystem.class.getName());
    }

    @Before
    public void setupJob() throws Exception {
      this.mockFS = mock(FileSystem.class);
      FileSystem s3 = new Path("s3://" + MockS3FileSystem.BUCKET + "/").getFileSystem(CONF);
      if (s3 instanceof MockS3FileSystem) {
        ((MockS3FileSystem) s3).setMock(mockFS);
      } else {
        throw new RuntimeException("Cannot continue: S3 not mocked");
      }

      this.job = new TaskAttemptContextImpl(CONF, TASK_ATTEMP_ID);
      job.getConfiguration().set(UPLOAD_UUID, UUID.randomUUID().toString());

      this.results = new TestUtil.ClientResults();
      this.errors = new TestUtil.ClientErrors();
      this.mockClient = TestUtil.newMockClient(results, errors);
    }

    public FileSystem getMockS3() {
      return mockFS;
    }

    public TaskAttemptContext getJob() {
      return job;
    }

    protected TestUtil.ClientResults getMockResults() {
      return results;
    }

    protected TestUtil.ClientErrors getMockErrors() {
      return errors;
    }

    protected S3Client getMockClient() {
      return mockClient;
    }

    abstract C newJobCommitter() throws Exception;
  }

  public abstract static class TaskCommitterTest<C extends OutputCommitter>
      extends JobCommitterTest<C> {
    private static final TaskAttemptID AID =
        new TaskAttemptID(new TaskID(JobCommitterTest.JOB_ID, TaskType.REDUCE, 2), 3);

    private C jobCommitter = null;
    private TaskAttemptContext tac = null;

    @Before
    public void setupTask() throws Exception {
      this.jobCommitter = newJobCommitter();
      jobCommitter.setupJob(getJob());

      this.tac = new TaskAttemptContextImpl(new Configuration(getJob().getConfiguration()), AID);

      // get the task's configuration copy so modifications take effect
      tac.getConfiguration().set("mapred.local.dir", "/tmp/local-0,/tmp/local-1");
    }

    protected C getJobCommitter() {
      return jobCommitter;
    }

    protected TaskAttemptContext getTAC() {
      return tac;
    }

    abstract C newTaskCommitter() throws Exception;
  }

  public static class ClientResults implements Serializable {
    // For inspection of what the committer did
    public final List<S3MultipartUploadRef> requests = Lists.newArrayList();
    public final List<String> uploads = Lists.newArrayList();
    public final Map<S3MultipartUploadRef, Integer> parts = Maps.newLinkedHashMap();
    public final Map<String, List<String>> tagsByUpload = Maps.newHashMap();
    public final List<S3MultipartUploadRef> commits = Lists.newArrayList();
    public final List<S3MultipartUploadRef> aborts = Lists.newArrayList();
    public final List<S3Ref> deletes = Lists.newArrayList();

    public List<S3MultipartUploadRef> getRequests() {
      return requests;
    }

    public List<String> getUploads() {
      return uploads;
    }

    public Map<S3MultipartUploadRef, Integer> getParts() {
      return parts;
    }

    public Map<String, List<String>> getTagsByUpload() {
      return tagsByUpload;
    }

    public List<S3MultipartUploadRef> getCommits() {
      return commits;
    }

    public List<S3MultipartUploadRef> getAborts() {
      return aborts;
    }

    public List<S3Ref> getDeletes() {
      return deletes;
    }
  }

  public static class ClientErrors {
    // For injecting errors
    public int failOnInit = -1;
    public int failOnUpload = -1;
    public int failOnCommit = -1;
    public int failOnAbort = -1;
    public boolean recover = false;

    public void failOnInit(int initNum) {
      this.failOnInit = initNum;
    }

    public void failOnUpload(int uploadNum) {
      this.failOnUpload = uploadNum;
    }

    public void failOnCommit(int commitNum) {
      this.failOnCommit = commitNum;
    }

    public void failOnAbort(int abortNum) {
      this.failOnAbort = abortNum;
    }

    public void recoverAfterFailure() {
      this.recover = true;
    }
  }

  public static S3Client newMockClient(final ClientResults results, final ClientErrors errors) {
    S3Client mockClient = mock(S3Client.class);
    final Object lock = new Object();

    when(mockClient.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
        .thenAnswer(
            (Answer<CreateMultipartUploadResponse>)
                invocation -> {
                  synchronized (lock) {
                    if (results.requests.size() == errors.failOnInit) {
                      if (errors.recover) {
                        errors.failOnInit(-1);
                      }
                      throw SdkException.builder()
                          .message("Fail on init " + results.requests.size())
                          .build();
                    }

                    String uploadId = UUID.randomUUID().toString();
                    CreateMultipartUploadRequest req =
                        invocation.getArgumentAt(0, CreateMultipartUploadRequest.class);
                    S3MultipartUploadRef ref =
                        S3MultipartUploadRef.of(req.bucket(), req.key(), uploadId);
                    results.requests.add(ref);
                    results.uploads.add(uploadId);
                    return newResult(ref);
                  }
                });

    when(mockClient.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
        .thenAnswer(
            (Answer<UploadPartResponse>)
                invocation -> {
                  synchronized (lock) {
                    if (results.parts.size() == errors.failOnUpload) {
                      if (errors.recover) {
                        errors.failOnUpload(-1);
                      }
                      throw SdkException.builder()
                          .message("Fail on upload " + results.parts.size())
                          .build();
                    }
                    UploadPartRequest req = invocation.getArgumentAt(0, UploadPartRequest.class);
                    results.parts.put(
                        S3MultipartUploadRef.of(req.bucket(), req.key(), req.uploadId()),
                        req.partNumber());
                    String etag = UUID.randomUUID().toString();
                    List<String> etags =
                        results.tagsByUpload.computeIfAbsent(
                            req.uploadId(), k -> Lists.newArrayList());
                    etags.add(etag);
                    return newResult(req, etag);
                  }
                });

    when(mockClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
        .thenAnswer(
            (Answer<CompleteMultipartUploadResponse>)
                invocation -> {
                  synchronized (lock) {
                    if (results.commits.size() == errors.failOnCommit) {
                      if (errors.recover) {
                        errors.failOnCommit(-1);
                      }
                      throw SdkException.builder()
                          .message("Fail on commit " + results.commits.size())
                          .build();
                    }
                    CompleteMultipartUploadRequest req =
                        invocation.getArgumentAt(0, CompleteMultipartUploadRequest.class);
                    results.commits.add(
                        S3MultipartUploadRef.of(req.bucket(), req.key(), req.uploadId()));
                    return newResult(req);
                  }
                });

    doAnswer(
            (Answer<Void>)
                invocation -> {
                  synchronized (lock) {
                    if (results.aborts.size() == errors.failOnAbort) {
                      if (errors.recover) {
                        errors.failOnAbort(-1);
                      }
                      throw SdkException.builder()
                          .message("Fail on abort " + results.aborts.size())
                          .build();
                    }

                    AbortMultipartUploadRequest req =
                        invocation.getArgumentAt(0, AbortMultipartUploadRequest.class);
                    results.aborts.add(
                        S3MultipartUploadRef.of(req.bucket(), req.key(), req.uploadId()));
                    return null;
                  }
                })
        .when(mockClient)
        .abortMultipartUpload(any(AbortMultipartUploadRequest.class));

    doAnswer(
            (Answer<Void>)
                invocation -> {
                  synchronized (lock) {
                    DeleteObjectRequest req =
                        invocation.getArgumentAt(0, DeleteObjectRequest.class);
                    results.deletes.add(S3Ref.of(req.bucket(), req.key()));
                    return null;
                  }
                })
        .when(mockClient)
        .deleteObject(any(DeleteObjectRequest.class));

    return mockClient;
  }

  private static CompleteMultipartUploadResponse newResult(CompleteMultipartUploadRequest req) {
    return CompleteMultipartUploadResponse.builder().build();
  }

  private static UploadPartResponse newResult(UploadPartRequest request, String etag) {
    return UploadPartResponse.builder().eTag(etag).build();
  }

  private static CreateMultipartUploadResponse newResult(S3MultipartUploadRef uploadRef) {
    return CreateMultipartUploadResponse.builder().uploadId(uploadRef.uploadId()).build();
  }

  public static void createTestOutputFiles(
      List<String> relativeFiles, Path attemptPath, Configuration conf) throws Exception {
    // create files in the attempt path that should be found by getTaskOutput
    FileSystem attemptFS = attemptPath.getFileSystem(conf);
    attemptFS.delete(attemptPath, true);
    for (String relative : relativeFiles) {
      // 0-length files are ignored, so write at least one byte
      OutputStream out = attemptFS.create(new Path(attemptPath, relative));
      out.write(34);
      out.close();
    }
  }

  /**
   * A convenience method to avoid a large number of @Test(expected=...) tests
   *
   * @param message A String message to describe this assertion
   * @param expected An Exception class that the Runnable should throw
   * @param callable A Callable that is expected to throw the exception
   */
  public static void assertThrows(
      String message, Class<? extends Exception> expected, Callable<?> callable) {
    assertThrows(message, expected, null, callable);
  }

  /**
   * A convenience method to avoid a large number of @Test(expected=...) tests
   *
   * @param message A String message to describe this assertion
   * @param expected An Exception class that the Runnable should throw
   * @param callable A Callable that is expected to throw the exception
   */
  public static void assertThrows(
      String message,
      Class<? extends Exception> expected,
      String expectedMsg,
      Callable<?> callable) {
    try {
      callable.call();
      Assert.fail("No exception was thrown (" + message + "), expected: " + expected.getName());
    } catch (Exception actual) {
      Assert.assertEquals(message, expected, actual.getClass());
      if (expectedMsg != null) {
        Assert.assertTrue(
            "Exception message should contain \""
                + expectedMsg
                + "\", but was: "
                + actual.getMessage(),
            actual.getMessage().contains(expectedMsg));
      }
    }
  }

  /**
   * A convenience method to avoid a large number of @Test(expected=...) tests
   *
   * @param message A String message to describe this assertion
   * @param expected An Exception class that the Runnable should throw
   * @param runnable A Runnable that is expected to throw the exception
   */
  public static void assertThrows(
      String message, Class<? extends Exception> expected, String expectedMsg, Runnable runnable) {
    try {
      runnable.run();
      Assert.fail("No exception was thrown (" + message + "), expected: " + expected.getName());
    } catch (Exception actual) {
      Assert.assertEquals(message, expected, actual.getClass());
      if (expectedMsg != null) {
        Assert.assertTrue(
            "Exception message should contain \""
                + expectedMsg
                + "\", but was: "
                + actual.getMessage(),
            actual.getMessage().contains(expectedMsg));
      }
    }
  }
}
