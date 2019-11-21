package top10;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

public class Top10Reducer extends Reducer<NullWritable, Text,NullWritable,Text> {
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        SortedMap<Double,String> map = new TreeMap<Double, String>();
        for (Text value : values) {
            String s = value.toString();
            String[] split = s.split(",");
            double v = Double.parseDouble(split[0]);
            map.put(v,value.toString());
            if (map.size() > 10){
                map.remove(map.firstKey());
            }
        }
        for (String value : map.values()) {
            System.out.println(value);
            Text text = new Text();
            text.set(value);
            context.write(NullWritable.get(),text);
        }
    }
}
