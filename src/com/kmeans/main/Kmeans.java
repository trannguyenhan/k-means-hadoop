package com.kmeans.main;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.kmeans.points.PointWritable;
import com.kmeans.points.PointService;

public class Kmeans {
	private static int k = 3; // 3 clusters
	private static List<PointWritable> listPoints;

	public Kmeans() throws IOException {
		listPoints = PointService.getListPoints("data/input.txt");
	}

	/*
	 * Input is file text The method convert input from text file to sequence file
	 */
	public void convertTextToSequenceFile(Configuration conf) throws IOException {
		String pathIn = conf.get("pathIn");
		String pathCenter = conf.get("pathCenter");
		
		SequenceFile.Writer writer = null;
		writer = SequenceFile.createWriter(conf, Writer.file(new Path(pathIn)), Writer.keyClass(PointWritable.class),
				Writer.valueClass(PointWritable.class));

		// first, center default is (0,0)
		// we will assign center default to every point
		for (PointWritable p : listPoints) {
			writer.append(new PointWritable(new IntWritable(0), new IntWritable(0)), p);
		}

		// random first k point center, write them to sequence file pathCenter
		writer = SequenceFile.createWriter(conf, Writer.file(new Path(pathCenter)),
				Writer.keyClass(PointWritable.class), Writer.valueClass(PointWritable.class));
		Random rand = new Random();
		int numberOfPoint = listPoints.size();
		for (int i = 0; i < k; i++) {
			int rand_num = rand.nextInt(numberOfPoint-1);
			PointWritable randomPoint = listPoints.get(rand_num);
			writer.append(randomPoint, new PointWritable(new IntWritable(0), new IntWritable(0)));
		}

		writer.close();
	}
	
	public static void main(String[] args)
			throws NumberFormatException, IOException, ClassNotFoundException, InterruptedException {
		String out = args[0];
		String in = "data/sequence/input.seq";
		String cen = "data/sequence/cen.seq";

		Kmeans kmeans = new Kmeans();
		Configuration conf = new Configuration();

		conf.set("pathIn", in);
		conf.set("pathCenter", cen);
		conf.set("pathOut", out);

		kmeans.convertTextToSequenceFile(conf);
		Job job = Job.getInstance(conf, "K-Means MapReduce");
		job.setJarByClass(Kmeans.class);
		job.setMapperClass(KmeansMap.class);
		job.setCombinerClass(KmeansReduce.class);
		job.setReducerClass(KmeansReduce.class);
		//job.setNumReduceTasks(1);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(PointWritable.class);
		job.setOutputValueClass(PointWritable.class);

		FileInputFormat.addInputPath(job, new Path(in));
		FileSystem fs = FileSystem.get(conf); // delete file output when it exists
		if (fs.exists(new Path(out))) {
			fs.delete(new Path(out), true);
		}

		FileOutputFormat.setOutputPath(job, new Path(out));

		if(job.waitForCompletion(true)) {
			System.out.println("SUCCESSFUL");
		} else {
			System.out.println("FAIL!");
		}
	}
}
