package back_index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class BackMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text k = new Text();

    private IntWritable v = new IntWritable(1);

    private String name;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        name = ((FileSplit) context.getInputSplit()).getPath().getName();
    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");

        for (String s : split) {
            k.set(this.name + "-" + s);
            context.write(k, v);
        }
    }
}
