package ma.enset.oneParam;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class KmeansDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf,"One Param");
        job.setJarByClass(KmeansDriver.class);
        job.setMapperClass(KmeansMapper.class);
        job.setReducerClass(KmeansReducer.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.addCacheFile(new URI("hdfs://localhost:9000/input/centers.txt"));
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);

    }
}
