package back_index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.io.IOException;

public class BackDriver {

    private String base = "G:\\dataagain\\hadoop_test\\src\\main\\java\\back_index\\file";

    @Test
    public void run() throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        job.setJarByClass(BackDriver.class);

        job.setMapperClass(BackMapper1.class);
        job.setReducerClass(BackReducer1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(base + "\\input"));
        FileOutputFormat.setOutputPath(job, new Path(base + "\\mid"));

        boolean b = job.waitForCompletion(true);
//        System.exit(b ? 0 : 1);

        if (b) {
            secondMR();
        }
    }

    private void secondMR() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(BackDriver.class);

        job.setMapperClass(BackMapper2.class);
        job.setReducerClass(BackReducer2.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(base + "\\mid"));
        FileOutputFormat.setOutputPath(job, new Path(base + "\\out"));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
    }

}
