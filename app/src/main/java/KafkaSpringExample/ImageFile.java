       
        
package KafkaSpringExample;

import lombok.Getter;
import lombok.ToString;

/**
 *
 * @author MC03353
 */

@Getter
@ToString
class ImageFile {
    
    private String mainClass;
    private String fileName;
    
    public ImageFile(){
        
    }
    
    public ImageFile(String mainClass, String fileName){
        this.mainClass = mainClass;
        this.fileName = fileName;
    }
    
    
}
