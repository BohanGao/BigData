import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PhaseOneReducer extends Reducer<Text, Text, Text, Text> {
    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        for(Text value:values){
            sb.append(value.toString());
            sb.append(",");
        }
        if(sb.length()>0){
            sb.deleteCharAt(sb.length()-1);
        }
        v.set(sb.toString());
        context.write(key, v);
    }
}
