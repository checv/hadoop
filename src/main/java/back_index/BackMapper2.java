package back_index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BackMapper2 extends Mapper<LongWritable, Text, Text, Text> {

    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] name_count = value.toString().split("\t");
        String[] file_word = name_count[0].split("-");
        k.set(file_word[1]);
        v.set(file_word[0] + "-->" + name_count[1]);
        context.write(k, v);
    }
}
