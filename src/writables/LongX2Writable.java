package writables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LongX2Writable implements Writable {

    private long a;
    private long b;

    public LongX2Writable(int a, int b) {
        this.a = a;
        this.b = b;
    }

    LongX2Writable() {

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(a);
        dataOutput.writeLong(b);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        a = dataInput.readLong();
        b = dataInput.readLong();
    }

    public long getA() {
        return a;
    }

    public void setA(long a) {
        this.a = a;
    }

    public long getB() {
        return b;
    }

    public void setB(long b) {
        this.b = b;
    }

    @Override
    public String toString() {
        return Long.toString(a) + '\t' + Long.toString(b);
    }
}
