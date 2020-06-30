import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

public class TestCompress {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String format = "org.apache.hadoop.io.compress.BZip2Codec";
//        String format = "org.apache.hadoop.io.compress.GzipCodec";
//        String format = "org.apache.hadoop.io.compress.DefaultCodec";
        testCompress("/Users/bohangao/Projects/BigData/Hadoop/Compress/test_data/hello.txt", format);
    }

    private static void testCompress(String filePath, String format) throws IOException, ClassNotFoundException {
        //get input stream
        FileInputStream fis = new FileInputStream(new File(filePath));

        //get codec
        Class classCodec = Class.forName(format);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(classCodec, new Configuration());

        //get output stream
        FileOutputStream fos = new FileOutputStream(new File(filePath + codec.getDefaultExtension()));
        CompressionOutputStream cos = codec.createOutputStream(fos);

        //copy content from input stream to output stream
        IOUtils.copyBytes(fis, cos, 1024 * 1024, false);

        //close resources
        IOUtils.closeStream(cos);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }
}
