package back_index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class BackReducer2 extends Reducer<Text, Text, Text, Text> {

    private Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();
        for (Text value : values) {
            sb.append(value).append(" ");
        }
        // remove the last blank
        String retValue = sb.toString();
        v.set(retValue.substring(0, retValue.lastIndexOf(" ")));
        context.write(key, v);
    }
}
