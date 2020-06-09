import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WebUrlReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        String url = key.toString()+"\r\n";
        Text k = new Text();
        k.set(url);
        for (NullWritable value : values) {
            context.write(k, NullWritable.get());
        }
    }
}
