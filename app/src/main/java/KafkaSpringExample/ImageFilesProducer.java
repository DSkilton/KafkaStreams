package KafkaSpringExample;

import java.lang.System.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

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

    
}
