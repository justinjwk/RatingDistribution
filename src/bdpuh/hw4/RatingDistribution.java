package bdpuh.hw4;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RatingDistribution {
    public static void main(String args[])  {

//        String inputPath = args[0];
//        String outputPath = args[1];

        String inputPath = "/movie-ratings";
        String outputPath = "/movie-rating-distribution";

        Job ratingDistributionJob = null;

        Configuration conf = new Configuration();

        //conf.setInt("mapred.reduce.tasks", 1);

        System.out.println ("======");
        try {
            ratingDistributionJob = new Job(conf, "RatingDistribution");
        } catch (IOException ex) {
            Logger.getLogger(RatingDistribution.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }


        // Specify the Input path
        try {
            FileInputFormat.addInputPath(ratingDistributionJob, new Path(inputPath));
        } catch (IOException ex) {
            Logger.getLogger(RatingDistribution.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }

        // Set the Input Data Format
        ratingDistributionJob.setInputFormatClass(TextInputFormat.class);

        // Set the Mapper and Reducer Class
        ratingDistributionJob.setMapperClass(RatingDistributionMapper.class);
        ratingDistributionJob.setReducerClass(RatingDistributionReducer.class);

        // Set the Jar file
        ratingDistributionJob.setJarByClass(bdpuh.hw4.RatingDistribution.class);

        // Set the Output path
        FileOutputFormat.setOutputPath(ratingDistributionJob, new Path(outputPath));

        // Set the Output Data Format
        ratingDistributionJob.setOutputFormatClass(TextOutputFormat.class);

        // Set the Output Key and Value Class
        ratingDistributionJob.setOutputKeyClass(Text.class);
        ratingDistributionJob.setOutputValueClass(IntWritable.class);


        //ratingDistributionJob.setNumReduceTasks(3);


        try {
            ratingDistributionJob.waitForCompletion(true);
        } catch (IOException ex) {
            Logger.getLogger(RatingDistribution.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(RatingDistribution.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(RatingDistribution.class.getName()).log(Level.SEVERE, null, ex);
        }



    }

}
