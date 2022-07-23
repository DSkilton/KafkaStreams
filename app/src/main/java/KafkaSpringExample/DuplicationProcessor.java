/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package KafkaSpringExample;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

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

    @Value("$spring.cloud.stream.bindings.input1.destination}")
    private String input1;

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
                .groupByKey(Grouped.with(Serdes.Long(),Serdes.String()).withName("g1"))
                .reduce((value1, value2) -> {
                    Set<String> s1 = new HashSet<String>(Arrays.asList(value1.split("\\|")));
                    s1.addAll(new HashSet<String>(Arrays.asList(value2.split("\\|"))));
                    return String.join("|", s1);
                });
        return imageFileStream;
    }

}
