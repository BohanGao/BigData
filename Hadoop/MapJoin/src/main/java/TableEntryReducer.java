import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TableEntryReducer extends Reducer<TableEntryBean, NullWritable, TableEntryBean, NullWritable> {
    @Override
    protected void reduce(TableEntryBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable value : values) {
            context.write(key, NullWritable.get());
        }
    }
}
