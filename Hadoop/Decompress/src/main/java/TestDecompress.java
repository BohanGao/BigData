import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;

import java.io.*;

public class TestDecompress {
    public static void main(String[] args) throws IOException {
        String file = "/Users/bohangao/Projects/BigData/Hadoop/Decompress/test_data/hello.txt.bz2";
//        String file = "/Users/bohangao/Projects/BigData/Hadoop/Decompress/test_data/hello.txt.gz";
//        String file = "/Users/bohangao/Projects/BigData/Hadoop/Decompress/test_data/hello.txt.deflate";
        testDecompress(file);
    }

    private static void testDecompress(String filePath) throws IOException {
        //check if codec of the file is supported
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(filePath));

        if (codec == null) {
            System.out.println("cannot process decompress to the input file.");
            return;
        }

        //get input stream
        FileInputStream fis = new FileInputStream(new File(filePath));
        CompressionInputStream cis = codec.createInputStream(fis);

        //get output stream
        FileOutputStream fos = new FileOutputStream(new File(filePath + ".decode"));

        //copy content from input stream to output stream
        IOUtils.copyBytes(cis, fos, 1024 * 1024, false);

        //close resources
        IOUtils.closeStream(fos);
        IOUtils.closeStream(cis);
        IOUtils.closeStream(fis);
    }
}
