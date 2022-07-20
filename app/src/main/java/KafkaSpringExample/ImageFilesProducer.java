package KafkaSpringExample;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 *
 * @author MC03353
 */
@Component
public class ImageFilesProducer {

    static Logger logger = LoggerFactory.getLogger(ImageFilesProducer.class);

    @Autowired
    private KafkaTemplate<String, ImageFile> producer;

    @Value("${spring.cloud.stream.bindings.input1.destination}")
    private String topic;

    @Value("${dataset.input.path")
    private String inputPath;

    @Value("${dataset.files.extensions}")
    private String[] extensions;

    public void startProducer() {
        ProducerThread producerThread = new ProducerThread();
        producerThread.start();
    }

    class ProducerThread extends Thread {

        public void run() {
            try {
                Path rootPath = Paths.get(inputPath);
                if (!Files.isDirectory(rootPath)) {
                    throw new RuntimeException(String.format("Data input rootPath is not a directory %s", inputPath));
                }
                DirectoryStream<Path> classStream = Files.newDirectoryStream(rootPath, entry -> Files.isDirectory(entry));
                for (Path classPath : classStream) {
                    DirectoryStream<Path> imageStream = Files.newDirectoryStream(classPath,
                            entry -> Arrays.stream(extensions).anyMatch(entry.toString()::endsWith));

                    for (Path imagePath : imageStream) {
                        sendMessage(new ImageFile(classPath.getFileName().toString(), imagePath.toString()));
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("IO Exception while ready images", e);
            }
        }

        private void sendMessage(ImageFile imageFile) {
            long startTime = System.currentTimeMillis();
            var message = MessageBuilder
                    .withPayload(imageFile)
                    .setHeader(KafkaHeaders.TOPIC, topic)
                    .setHeader(KafkaHeaders.MESSAGE_KEY, UUID.randomUUID().toString())
                    .build();
            // Send asynchronously
            producer.send(message).addCallback(new ProducerCallBack(startTime, imageFile));
        }
    }
    
    static class ProducerCallBack implements ListenableFutureCallback<SendResult<String, ImageFile>>{

        private final ImageFile imageFile;
        private final long startTime;
        
        private ProducerCallBack(long startTime, ImageFile imageFile) {
            this.imageFile = imageFile;
            this.startTime = startTime;
        }

        @Override
        public void onFailure(Throwable e) {
            logger.error("Fail producing image: ", e);
        }
        
        @Override
        public void onSuccess(SendResult<String, ImageFile> t) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            logger.info("Message {} sent in {} ms", imageFile, elapsedTime);
        }

        
        
    }
}
