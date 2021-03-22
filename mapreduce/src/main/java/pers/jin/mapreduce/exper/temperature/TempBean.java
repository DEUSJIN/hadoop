package pers.jin.mapreduce.exper.temperature;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: DEUSJIN
 * @Date: 2021/3/19 15:46
 */
public class TempBean implements WritableComparable<TempBean> {
    private int month;
    private int temp;
    private List<Integer> list = new ArrayList<>();

    public TempBean(int month, int temp) {
        this.month = month;
        this.temp = temp;
    }

    public List<Integer> getList() {
        return list;
    }

    public void setList(List<Integer> list) {
        this.list = list;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getTemp() {
        return temp;
    }

    public void setTemp(int temp) {
        this.temp = temp;
    }

    public TempBean() {
    }

    @Override
    public int compareTo(TempBean o) {
        return o.month - month;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(month);
        out.writeInt(temp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        month = in.readInt();
        temp = in.readInt();
    }
}
