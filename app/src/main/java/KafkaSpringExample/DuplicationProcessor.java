/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package KafkaSpringExample;

/**
 *
 * @author MC03353
 * This class will identify duplicate image. For that, we receive the input images (input1) 
 * and group the files by size. Since each image was resized to the same size, we compare 
 * each identical-size file’s content and stream the same size and content files grouped by 
 * size to output1. Each record on output1 is of the form of key: size as long, value: a 
 * string of concatenated files (e.g. “file1|file2”).
 */
public class DuplicationProcessor extends  {
    
}
