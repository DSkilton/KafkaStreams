/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package KafkaSpringExample;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.logging.log4j.util.Strings;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 *
 * @author MC03353 This class will identify duplicate image. For that, we
 * receive the input images (input1) and group the files by size. Since each
 * image was resized to the same size, we compare each identical-size file’s
 * content and stream the same size and content files grouped by size to
 * output1. Each record on output1 is of the form of key: size as long, value: a
 * string of concatenated files (e.g. “file1|file2”).
 */
@Configuration
@EnableKafka
@EnableKafkaStreams
public class DuplicationProcessor extends BaseProcessor {

    static Logger logger = LoggerFactory.getLogger(DuplicationProcessor.class);

    @Value("${duplicationProcessor.sendMessageDelay}")
    public int sendMessageDelay;

    @Value("$spring.cloud.stream.bindings.input1.destination}")
    private String input1;

    @Value("$spring.cloud.stream.bindings.input2.destination}")
    private String input2;

    @Value("$spring.cloud.stream.bindings.output1.destination")
    private String output1;

    @Value("$spring.cloud.stream.bindings.output1.destination")
    private String output2;

    @Bean
    public KStream<String, ImageFile> detectDuplicateImages(StreamsBuilder streamsBuilder) {
        KStream<String, ImageFile> imageFileStream = streamsBuilder.stream(input1, Consumed.with(Serdes.String(), new JsonSerde<>(ImageFile.class)));
        imageFileStream
                .map((key, imageFile) -> {
                    try {
                        logger.info("Reading file.. {}", imageFile.getFileName());
                        return new KeyValue<>(Files.size(Paths.get(imageFile.getFileName())), imageFile.getFileName());
                    } catch (IOException e) {
                        return new KeyValue<>(0L, imageFile.getFileName());
                    }
                })
                .groupByKey(Grouped.with(Serdes.Long(), Serdes.String()).withName("g1"))
                .reduce((value1, value2) -> {
                    Set<String> s1 = new HashSet<String>(Arrays.asList(value1.split("\\|")));
                    s1.addAll(new HashSet<String>(Arrays.asList(value2.split("\\|"))));
                    return String.join("|", s1);
                }, Materialized.with(Serdes.Long(), Serdes.String()))
                .toStream()
                .filter((Key, value) -> Strings.isNotEmpty(value) && value.contains("|"))
                .flatMapValues((key, value) -> Arrays.asList(value.split("\\|")))
                .groupByKey(Grouped.with(Serdes.Long(), Serdes.String()).withName("g2"))
                .windowedBy(UnlimitedWindows.of())
                .reduce((file1, file2) -> {
                    try {
                        String comp1 = file1;
                        if (file1.contains("|")) {
                            comp1 = file1.split("\\|")[0];
                        }
                        if (!com.google.common.io.Files.equal(new File(comp1), new File(file2))) {
                            return null;
                        } else {
                            return String.join("|", file1, file2);
                        }
                    } catch (IOException e) {
                        return null;
                    }
                }, Materialized.with(Serdes.Long(), Serdes.String()))
                .toStream()
                .filter((key, value) -> Strings.isNotEmpty(value) && value.contains("|"))
                .map((key, value) -> new KeyValue<>(key.key(), value))
                .to(output1, Produced.with(Serdes.Long(), Serdes.String()));
        ;
        return imageFileStream;
    }

    public KStream<Long, String> removeDuplicateImages(StreamsBuilder streamsBuilder) {
        KStream<Long, String> stream = streamsBuilder.stream(input2, Consumed.with(Serdes.Long(), Serdes.String()));

        stream.map((key, identical_files) -> {
            Set<String> classes = new HashSet<>();
            Set<String> files = new HashSet<>(Arrays.asList(identical_files.split("\\|")));

            for (String fileName : files) {
                classes.add(DatasetUtils.getClassName(fileName));
            }

            if (classes.size() == 1) {
                logger.info("Mark for delete [{}] same class files", files.size() - 1);
                files.remove(files.iterator().next());
            } else if (classes.size() > 1) {
                logger.info("Mark for delete all [{}] classes files", files.size());
            } else {
                logger.info("No class marked for deletion");
                return new KeyValue<Integer, Integer>(0, 0);
            }

            return new KeyValue<Integer, Integer>(0, files.size());
        })
                .groupByKey(Grouped.<Integer, Integer>as("g3").withKeySerde(Serdes.Integer()))
                .reduce(Integer::sum, Materialized.with(Serdes.Integer(), Serdes.Integer()))
                .toStream()
                .peek((key, value) -> logger.info("[{}] files deleted so far", value))
                .process(() -> new AbstractProcessor<Integer, Integer>() {
            private final AtomicInteger key = new AtomicInteger(0);
            private final AtomicInteger value = new AtomicInteger(0);
            private final ScheduledExecutorService scheduler = Executors
                    .newScheduledThreadPool(1);
            private KafkaTemplate<Integer, Integer> producer;

            @Override
            public void init(ProcessorContext context) {
                super.init(context);
                producer = producerTemplate();
                scheduler.schedule(() -> {
                    logger.info("sending [{}] files deleted message", value.get());
                    producer.send(output2, key.get(), value.get()).addCallback(new ListenableFutureCallback<SendResult<Integer, Integer>>() {
                        @Override
                        public void onFailure(Throwable ex) {
                            logger.error("Failed to send message", ex);
                        }

                        @Override
                        public void onSuccess(SendResult<Integer, Integer> result) {
                            logger.info("Message sent successfully {}", result);
                        }
                    });
                    producer.flush();
                    scheduler.shutdown();
                    this.close();
                }, sendMessageDelay, TimeUnit.SECONDS);
            }

            @Override
            public void process(Integer key, Integer value) {
                this.key.set(key);
                this.value.set(value);
            }
            
            @Override
            public void close(){
                super.close();
                logger.info("Closing processor");
            }
        });
        return stream;
    }
    
    
}
