package top10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jets3t.service.multithread.ServiceEvent;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

public class Top10Mapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    //重写setup和cleanup函数
    //这样使map不按照一行处理，而是将所有的数据处理完之后再cleanup
    private static SortedMap<Double, String> top10cats = new TreeMap<Double, String>();
    private int N = 10;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split(",");
        double v = Double.parseDouble(split[0]);
        top10cats.put(v,value.toString());
        if (top10cats.size() > 10){
            top10cats.remove(top10cats.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String value : top10cats.values()) {
            Text text = new Text();
            text.set(value);
            context.write(NullWritable.get(),text);
            System.out.println(value);
        }
        System.out.println(1);
    }
}
