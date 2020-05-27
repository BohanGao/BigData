import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {

    FileSplit split;
    Configuration configuration;
    Text k;
    BytesWritable v;
    boolean inProgress = true;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.split = (FileSplit)inputSplit;

        this.configuration = taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!inProgress){
            return false;
        }

        //1. get file system
        Path path = split.getPath();
        FileSystem fileSystem = path.getFileSystem(this.configuration);

        //2. get input stream
        FSDataInputStream fis = fileSystem.open(path);

        //3. copy file
        byte[] buffer = new byte[(int) split.getLength()];
        IOUtils.readFully(fis, buffer, 0, buffer.length);

        //4. get v
        v.set(buffer, 0, buffer.length);

        //5. get k
        k.set(path.toString());

        //6. close stream
        IOUtils.closeStream(fis);

        inProgress = false;

        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
