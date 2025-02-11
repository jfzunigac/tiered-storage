package com.pinterest.kafka.tieredstorage.uploader.leadership;

import com.pinterest.kafka.tieredstorage.common.discovery.s3.MockS3StorageServiceEndpointProvider;
import com.pinterest.kafka.tieredstorage.uploader.DirectoryTreeWatcher;
import com.pinterest.kafka.tieredstorage.uploader.KafkaEnvironmentProvider;
import com.pinterest.kafka.tieredstorage.uploader.MultiThreadedS3FileUploader;
import com.pinterest.kafka.tieredstorage.uploader.S3FileUploader;
import com.pinterest.kafka.tieredstorage.uploader.SegmentUploaderConfiguration;
import com.pinterest.kafka.tieredstorage.uploader.TestBase;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static java.nio.file.Files.createTempDirectory;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test for {@link LeadershipWatcher}
 */
public class TestLeadershipWatcher extends TestBase {
    private static final Set<TopicPartition> watchedTopicPartitions = new HashSet<>();
    private static final Set<TopicPartition> currentLeadingPartitions = new HashSet<>();
    private LeadershipWatcher mockLeadershipWatcher;

    @BeforeEach
    @Override
    public void setup() throws Exception {
        super.setup();

        // environment provider setup
        KafkaEnvironmentProvider environmentProvider = createTestEnvironmentProvider("sampleZkConnect", "sampleLogDir");
        environmentProvider.load();

        // override s3 client
        overrideS3ClientForFileDownloader(s3Client);
        overrideS3AsyncClientForFileUploader(s3AsyncClient);

        // endpoint provider setup
        MockS3StorageServiceEndpointProvider endpointProvider = new MockS3StorageServiceEndpointProvider();
        endpointProvider.initialize(TEST_CLUSTER);

        // s3 uploader setup
        SegmentUploaderConfiguration config = getSegmentUploaderConfiguration(TEST_CLUSTER);
        S3FileUploader s3FileUploader = new MultiThreadedS3FileUploader(endpointProvider, config, environmentProvider);

        // create watchers
        DirectoryTreeWatcher directoryTreeWatcher = new MockDirectoryTreeWatcher(watchedTopicPartitions, s3FileUploader, config, environmentProvider);
        mockLeadershipWatcher = new MockLeadershipWatcher(directoryTreeWatcher, config, environmentProvider);
    }

    /**
     * Test {@link LeadershipWatcher#applyCurrentState()} method
     */
    @Test
    void testApplyCurrentState() throws Exception {
        // test initial state
        mockLeadershipWatcher.applyCurrentState();
        assertEquals(0, watchedTopicPartitions.size());

        // test addition
        currentLeadingPartitions.add(new TopicPartition(TEST_TOPIC_A, 0));
        currentLeadingPartitions.add(new TopicPartition(TEST_TOPIC_A, 1));
        currentLeadingPartitions.add(new TopicPartition(TEST_TOPIC_A, 2));
        mockLeadershipWatcher.applyCurrentState();
        assertEquals(3, watchedTopicPartitions.size());
        assertEquals(currentLeadingPartitions, watchedTopicPartitions);

        // test removal
        currentLeadingPartitions.remove(new TopicPartition(TEST_TOPIC_A, 0));
        mockLeadershipWatcher.applyCurrentState();
        assertEquals(2, watchedTopicPartitions.size());
        assertEquals(currentLeadingPartitions, watchedTopicPartitions);

        // test addition and removal
        currentLeadingPartitions.add(new TopicPartition(TEST_TOPIC_A, 0));
        currentLeadingPartitions.add(new TopicPartition(TEST_TOPIC_A, 3));
        currentLeadingPartitions.add(new TopicPartition(TEST_TOPIC_A, 4));
        currentLeadingPartitions.add(new TopicPartition(TEST_TOPIC_A, 5));
        currentLeadingPartitions.remove(new TopicPartition(TEST_TOPIC_A, 1));
        mockLeadershipWatcher.applyCurrentState();
        assertEquals(5, watchedTopicPartitions.size());
        assertEquals(currentLeadingPartitions, watchedTopicPartitions);

        // test no change
        mockLeadershipWatcher.applyCurrentState();
        assertEquals(5, watchedTopicPartitions.size());
        assertEquals(currentLeadingPartitions, watchedTopicPartitions);
    }

    /**
     * Test {@link LeadershipWatcher#applyCurrentState()} method
     */
    @Test
    void testMultiplePartitionsUnwatched() throws Exception {
        // test initial state
        // environment provider setup
        Path topLevelPath = createTempDirectory("testMultiplePartitionsUnwatched");
        KafkaEnvironmentProvider environmentProvider = createTestEnvironmentProvider("sampleZkConnect", topLevelPath.toString());
        environmentProvider.load();

        // override s3 client
        overrideS3ClientForFileDownloader(s3Client);
        overrideS3AsyncClientForFileUploader(s3AsyncClient);

        // endpoint provider setup
        MockS3StorageServiceEndpointProvider endpointProvider = new MockS3StorageServiceEndpointProvider();
        endpointProvider.initialize(TEST_CLUSTER);

        // s3 uploader setup
        SegmentUploaderConfiguration config = getSegmentUploaderConfiguration(TEST_CLUSTER);
        S3FileUploader s3FileUploader = new MultiThreadedS3FileUploader(endpointProvider, config, environmentProvider);

        // create watchers
        DirectoryTreeWatcher directoryTreeWatcher = new MockDirectoryTreeWatcherWithActualDirectories(watchedTopicPartitions, s3FileUploader, config, environmentProvider);
        mockLeadershipWatcher = new MockLeadershipWatcher(directoryTreeWatcher, config, environmentProvider);
        mockLeadershipWatcher.applyCurrentState();
        assertEquals(0, watchedTopicPartitions.size());

        // test addition
        for (int i = 0; i < 100; i++) {
            boolean success = new File(topLevelPath.toFile(), new TopicPartition(TEST_TOPIC_A, i).toString()).mkdirs();
            if (!success) {
                throw new IOException("Failed to create directory for " + new TopicPartition(TEST_TOPIC_A, i));
            }
            currentLeadingPartitions.add(new TopicPartition(TEST_TOPIC_A, i));
        }
        mockLeadershipWatcher.applyCurrentState();
        assertEquals(100, watchedTopicPartitions.size());
        assertEquals(currentLeadingPartitions, watchedTopicPartitions);

        // test removal
        for (int i = 0; i < 100; i++) {
            currentLeadingPartitions.remove(new TopicPartition(TEST_TOPIC_A, i));
        }

        mockLeadershipWatcher.applyCurrentState();
        assertEquals(0, watchedTopicPartitions.size());
        assertEquals(currentLeadingPartitions, watchedTopicPartitions);

        // test addition
        for (int i = 0; i < 100; i++) {
            currentLeadingPartitions.add(new TopicPartition(TEST_TOPIC_A, i));
        }

        mockLeadershipWatcher.applyCurrentState();
        assertEquals(100, watchedTopicPartitions.size());
        assertEquals(currentLeadingPartitions, watchedTopicPartitions);
    }

    /**
     * Mock {@link LeadershipWatcher} for testing
     */
    private static class MockLeadershipWatcher extends LeadershipWatcher {

        public MockLeadershipWatcher(DirectoryTreeWatcher directoryTreeWatcher, SegmentUploaderConfiguration config, KafkaEnvironmentProvider environmentProvider) throws IOException, InterruptedException {
            super(directoryTreeWatcher, config, environmentProvider);
        }

        @Override
        protected void initialize() throws IOException, InterruptedException {
            // no-op
        }

        @Override
        protected Set<TopicPartition> queryCurrentLeadingPartitions() throws Exception {
            return currentLeadingPartitions;
        }
    }

    /**
     * Mock {@link DirectoryTreeWatcher} for testing
     */
    protected static class MockDirectoryTreeWatcher extends DirectoryTreeWatcher {

        private final Set<TopicPartition> watchedTopicPartitions;

        public MockDirectoryTreeWatcher(Set<TopicPartition> watchedTopicPartitions, S3FileUploader s3FileUploader, SegmentUploaderConfiguration config, KafkaEnvironmentProvider environmentProvider) throws Exception {
            super(s3FileUploader, config, environmentProvider);
            this.watchedTopicPartitions = watchedTopicPartitions;
        }

        @Override
        public void watch(TopicPartition topicPartition) {
            watchedTopicPartitions.add(topicPartition);
        }

        @Override
        public void unwatch(TopicPartition topicPartition) {
            watchedTopicPartitions.remove(topicPartition);
        }
    }


    /**
     * Mock {@link DirectoryTreeWatcher} for testing.
     */
    protected static class MockDirectoryTreeWatcherWithActualDirectories extends DirectoryTreeWatcher {

        private final Set<TopicPartition> watchedTopicPartitions;

        public MockDirectoryTreeWatcherWithActualDirectories(Set<TopicPartition> watchedTopicPartitions, S3FileUploader s3FileUploader, SegmentUploaderConfiguration config, KafkaEnvironmentProvider environmentProvider) throws Exception {
            super(s3FileUploader, config, environmentProvider);
            this.watchedTopicPartitions = watchedTopicPartitions;
        }

        @Override
        public void watch(TopicPartition topicPartition) {
            watchedTopicPartitions.add(topicPartition);
            watchPath(topLevelPath.resolve(topicPartition.toString()));
        }

        @Override
        public void unwatch(TopicPartition topicPartition) {
            watchedTopicPartitions.remove(topicPartition);
            unwatchPath(topLevelPath.resolve(topicPartition.toString()));
        }
    }
}
