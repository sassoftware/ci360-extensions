package com.sas.ci360.agent.util;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
/*import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;*/
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.Exceptions.AppendSerializtionError;
import com.google.cloud.bigquery.storage.v1.Exceptions.StorageException;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors.DescriptorValidationException;

import io.grpc.Status;
import io.grpc.Status.Code;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Phaser;
import javax.annotation.concurrent.GuardedBy;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleBQUtil {

    private static final Logger logger = LoggerFactory.getLogger(GoogleBQUtil.class);

    private static DataWriter writer;
    private static TableName parentTable;
    

    public static void initialize(String projectId, String datasetName, String tableName)
            throws DescriptorValidationException, IOException, InterruptedException {

        logger.info("Initializing GBQ writer/connection.");
        /*GoogleBQUtil.projectId = projectId;
        GoogleBQUtil.datasetName = datasetName;
        GoogleBQUtil.tableName = tableName;
*/
        parentTable = TableName.of(projectId, datasetName, tableName);

        writer = new DataWriter();

        // One time initialization for the worker.
        writer.initialize(parentTable);

    }

    public static void writeToDefaultStream(JSONArray jsonArr, String rowKey)
            throws DescriptorValidationException, InterruptedException, IOException {

        // maximum request size:
        // https://cloud.google.com/bigquery/quotas#write-api-limits
        writer.append(new AppendContext(jsonArr, 0, rowKey));

        logger.debug(rowKey + "Handed over the event to be written to DB.");
    }

    public static void cleanup() {
        // Final cleanup for the stream during worker teardown.
        writer.cleanup();
        logger.info("GBQ Lib cleanup.");
    }

    private static class AppendContext {

        JSONArray data;
        int retryCount = 0;
        String rowKey;

        AppendContext(JSONArray data, int retryCount, String rowKey) {
            this.data = data;
            this.retryCount = retryCount;
            this.rowKey = rowKey;
        }
    }

    private static class DataWriter {

        private static final ImmutableList<Code> RETRIABLE_ERROR_CODES = ImmutableList.of(
                Code.INTERNAL,
                Code.ABORTED,
                Code.CANCELLED,
                Code.FAILED_PRECONDITION,
                Code.DEADLINE_EXCEEDED,
                Code.UNAVAILABLE);

        // Track the number of in-flight requests to wait for all responses before
        // shutting down.
        private final Phaser inflightRequestCount = new Phaser(1);
        private final Object lock = new Object();
        private JsonStreamWriter streamWriter;

        @GuardedBy("lock")
        private RuntimeException error = null;

        public void initialize(TableName parentTable)
                throws DescriptorValidationException, IOException, InterruptedException {
            // Use the JSON stream writer to send records in JSON format. Specify the table
            // name to write
            // to the default stream.
            // For more information about JsonStreamWriter, see:
            // https://googleapis.dev/java/google-cloud-bigquerystorage/latest/com/google/cloud/bigquery/storage/v1/JsonStreamWriter.html
            logger.debug("Initializing Stream Writer for: " + parentTable.toString());
            streamWriter = JsonStreamWriter.newBuilder(parentTable.toString(), BigQueryWriteClient.create()).build();
        }

        public void append(AppendContext appendContext)
                throws DescriptorValidationException, IOException {
            synchronized (this.lock) {
                // If earlier appends have failed, we need to reset before continuing.
                if (this.error != null) {
                    logger.warn(appendContext.rowKey
                            + "Earlier appends have failed. Resetting before continuing. Error: " + error.getMessage());
                    throw this.error;
                }
            }
            // Append asynchronously for increased throughput.
            ApiFuture<AppendRowsResponse> future = streamWriter.append(appendContext.data);
            ApiFutures.addCallback(
                    future, new AppendCompleteCallback(this, appendContext), MoreExecutors.directExecutor());

            // Increase the count of in-flight requests.
            inflightRequestCount.register();
        }

        public void cleanup() {
            logger.debug("Clean up starting. Wait for all in-flight requests to complete.");
            // Wait for all in-flight requests to complete.
            inflightRequestCount.arriveAndAwaitAdvance();

            logger.debug("Clean up starting. Close the connection to the server.");
            // Close the connection to the server.
            streamWriter.close();

            // Verify that no error occurred in the stream.
            synchronized (this.lock) {
                if (this.error != null) {
                    logger.warn("Error occured in the stream. Error: " + error.getMessage());
                    throw this.error;
                }
            }
        }

        static class AppendCompleteCallback implements ApiFutureCallback<AppendRowsResponse> {

            private final DataWriter parent;
            private final AppendContext appendContext;

            public AppendCompleteCallback(DataWriter parent, AppendContext appendContext) {
                this.parent = parent;
                this.appendContext = appendContext;
            }

            public void onSuccess(AppendRowsResponse response) {
                AgentUtils.Event_Error_Count = 0;
                logger.info(appendContext.rowKey + "Append success to GBQ Table.");
                done();
            }

            public void onFailure(Throwable throwable) {
                logger.warn(appendContext.rowKey + "There was an error writing to the DB. Will try again.");
                logger.debug(
                        "retry count=" + appendContext.retryCount + ", max retry count=" + AgentUtils.MAX_RETRY_COUNT);
                // If the wrapped exception is a StatusRuntimeException, check the state of the
                // operation.
                // If the state is INTERNAL, CANCELLED, or ABORTED, you can retry. For more
                // information,
                // see:
                // https://grpc.github.io/grpc-java/javadoc/io/grpc/StatusRuntimeException.html

                Status status = Status.fromThrowable(throwable);
                logger.debug(appendContext.rowKey + "Status: " + status.toString());

                if (appendContext.retryCount < AgentUtils.MAX_RETRY_COUNT
                        && RETRIABLE_ERROR_CODES.contains(status.getCode())) {
                    appendContext.retryCount++;
                    try {
                        // Since default stream appends are not ordered, we can simply retry the
                        // appends.
                        // Retrying with exclusive streams requires more careful consideration.
                        this.parent.append(appendContext);
                        // Mark the existing attempt as done since it's being retried.
                        done();
                        return;
                    } catch (Exception e) {
                        // Fall through to return error.
                        AgentUtils.Event_Error_Count++;
                        logger.error(appendContext.rowKey + "Failed to retry append: %s\n", e);
                        AgentUtils.writeFiledEventToFile(appendContext.data, logger);
                    }
                }
                if (throwable instanceof AppendSerializtionError) {
                    AppendSerializtionError ase = (AppendSerializtionError) throwable;
                    logger.warn("AppendSerializtionError: " + ase.getMessage());
                    Map<Integer, String> rowIndexToErrorMessage = ase.getRowIndexToErrorMessage();
                    if (rowIndexToErrorMessage.size() > 0) {
                        // Omit the faulty rows
                        JSONArray dataNew = new JSONArray();
                        String rowKey = appendContext.rowKey;
                        for (int i = 0; i < appendContext.data.length(); i++) {
                            if (!rowIndexToErrorMessage.containsKey(i)) {
                                dataNew.put(appendContext.data.get(i));
                            } else {
                                // process faulty rows by placing them on a dead-letter-queue, for instance.
                                AgentUtils.Event_Error_Count++;
                                logger.warn(rowKey + "Error: This event will not be tried again. Error Message: "
                                        + rowIndexToErrorMessage.get(i));
                                AgentUtils.writeFiledEventToFile(appendContext.data, logger);
                            }
                        }

                        // Retry the remaining valid rows, but using a separate thread to
                        // avoid potentially blocking while we are in a callback.
                        if (dataNew.length() > 0) {
                            try {
                                this.parent.append(new AppendContext(dataNew, 0, rowKey));
                            } catch (DescriptorValidationException e) {
                                throw new RuntimeException(e);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        // Mark the existing attempt as done since we got a response for it
                        done();
                        return;
                    }
                }

                synchronized (this.parent.lock) {
                    if (this.parent.error == null) {
                        StorageException storageException = Exceptions.toStorageException(throwable);
                        this.parent.error = (storageException != null) ? storageException
                                : new RuntimeException(throwable);
                    }
                }
                done();
            }

            private void done() {
                logger.debug("Reduce the count of in-flight requests.\n");
                // Reduce the count of in-flight requests.
                this.parent.inflightRequestCount.arriveAndDeregister();
            }
        }
    }
}