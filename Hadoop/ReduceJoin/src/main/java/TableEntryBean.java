import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableEntryBean implements Writable {
    private String order_id;
    private String product_id;
    private String product_name;
    private int amount;
    private String flag;//indicates the entry is from which table

    public TableEntryBean() {
        super();
    }

    public TableEntryBean(String order_id, String product_id, String product_name, int amount, String flag) {
        this.order_id = order_id;
        this.product_id = product_id;
        this.product_name = product_name;
        this.amount = amount;
        this.flag = flag;
    }

    public String getOrder_id() {
        return order_id;
    }

    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }

    public String getProduct_id() {
        return product_id;
    }

    public void setProduct_id(String product_id) {
        this.product_id = product_id;
    }

    public String getProduct_name() {
        return product_name;
    }

    public void setProduct_name(String product_name) {
        this.product_name = product_name;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(order_id);
        dataOutput.writeUTF(product_id);
        dataOutput.writeUTF(product_name);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        order_id = dataInput.readUTF();
        product_id = dataInput.readUTF();
        product_name = dataInput.readUTF();
        amount = dataInput.readInt();
        flag = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return order_id + "\t" + product_name + "\t" + amount;
    }
}
