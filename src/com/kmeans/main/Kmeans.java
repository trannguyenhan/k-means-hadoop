package com.kmeans.main;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.kmeans.points.PointWritable;
import com.kmeans.points.PointService;

public class Kmeans {
	private int k;
	
	public Kmeans(int k) {
		this.k = k;
	}
	/*
	 * Write example k center before run MapReduce
	 */
	public void writeExempleCenter(Configuration conf) throws IOException {
		List<PointWritable> listPoints = PointService.getListPoints("data/input.txt");
		String pathCenter = conf.get("pathCenter");
		
		// random first k point center, write them to sequence file pathCenter
		SequenceFile.Writer writer = null;
		writer = SequenceFile.createWriter(conf, Writer.file(new Path(pathCenter)),
				Writer.keyClass(PointWritable.class), Writer.valueClass(PointWritable.class));
		Random rand = new Random();
		int numberOfPoint = listPoints.size();
		for (int i = 0; i < k; i++) {
			int rand_num = rand.nextInt(numberOfPoint-1);
			PointWritable randomPoint = listPoints.get(rand_num);
			writer.append(randomPoint, new PointWritable());
		}

		writer.close();
	}
	
	public static void main(String[] args)
			throws NumberFormatException, IOException, ClassNotFoundException, InterruptedException {
		String out = args[0];
		String in = "data/input.txt";
		String cen = "data/sequence/cen.seq";

		Kmeans kmeans = new Kmeans(3); // 3 cluster
		Configuration conf = new Configuration();

		conf.set("pathIn", in);
		conf.set("pathCenter", cen);
		conf.set("pathOut", out);

		kmeans.writeExempleCenter(conf);
		Job job = Job.getInstance(conf, "K-Means MapReduce");
		job.setJarByClass(Kmeans.class);
		job.setMapperClass(KmeansMap.class);
		job.setCombinerClass(KmeansReduce.class);
		job.setReducerClass(KmeansReduce.class);
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(PointWritable.class);
		job.setOutputValueClass(PointWritable.class);

		FileInputFormat.addInputPath(job, new Path(in));
		FileSystem fs = FileSystem.get(conf); // delete file output when it exists
		if (fs.exists(new Path(out))) {
			fs.delete(new Path(out), true);
		}

		FileOutputFormat.setOutputPath(job, new Path(out));

		if(job.waitForCompletion(true)) {
			System.out.println("SUBMIT WAS SUCCESSFUL!");
		} else {
			System.out.println("SUBMIT WAS FAIL!");
		}
	}
}
