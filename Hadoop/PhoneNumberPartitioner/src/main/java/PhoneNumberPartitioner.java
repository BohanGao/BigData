import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PhoneNumberPartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String phoneNumberPrefix = text.toString().substring(0, 3);
        int partition = 4;
        if (phoneNumberPrefix.equals("136")) {
            partition = 0;
        } else if (phoneNumberPrefix.equals("137")) {
            partition = 1;
        } else if (phoneNumberPrefix.equals("138")) {
            partition = 2;
        } else if (phoneNumberPrefix.equals("139")) {
            partition = 3;
        }
        return partition;
    }
}
