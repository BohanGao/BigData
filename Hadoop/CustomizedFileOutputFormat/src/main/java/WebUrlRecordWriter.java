import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class WebUrlRecordWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream out_baidu;
    FSDataOutputStream out_google;
    FSDataOutputStream out_other;

    public WebUrlRecordWriter(TaskAttemptContext taskAttemptContext) {
        try{
            FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());

            out_baidu = fs.create(new Path("/Users/bohangao/Projects/BigData/Hadoop/CustomizedFileOutputFormat/test_data/out/baidu.log"));
            out_google = fs.create(new Path("/Users/bohangao/Projects/BigData/Hadoop/CustomizedFileOutputFormat/test_data/out/google.log"));
            out_other = fs.create(new Path("/Users/bohangao/Projects/BigData/Hadoop/CustomizedFileOutputFormat/test_data/out/other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String k = text.toString();
        if(k.contains("baidu")){
            out_baidu.write(k.getBytes());
        } else if(k.contains("google")){
            out_google.write(k.getBytes());
        } else {
            out_other.write(k.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(out_baidu);
        IOUtils.closeStream(out_google);
        IOUtils.closeStream(out_other);
    }
}
