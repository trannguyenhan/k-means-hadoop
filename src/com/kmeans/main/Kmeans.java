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

import com.kmeans.points.Point;
import com.kmeans.points.PointService;

public class Kmeans {
	private int k = 3; // 3 clusters
	private List<Point> listPoints;

	public Kmeans() throws IOException {
		listPoints = PointService.getListPoints("data/input.txt");
	}
	
	public int getK() {
		return k;
	}

	public void setK(int k) {
		this.k = k;
	}

	public List<Point> getListPoints() {
		return listPoints;
	}

	public void setListPoints(List<Point> listPoints) {
		this.listPoints = listPoints;
	}

	/* Input is file text
	 * The method convert input from text file to sequence file
	 * */
	public void convertTextToSequenceFile(Configuration conf, String pathIn, String pathCenter)
			throws IOException {
		SequenceFile.Writer writer = null;
		writer = SequenceFile.createWriter(conf, Writer.file(new Path(pathIn)), 
		        Writer.keyClass(Point.class), Writer.valueClass(Point.class));
		
		// first, center default is (0,0)
		// we will assign center default to every point 
		for(Point p : listPoints) {
			writer.append(new Point(new IntWritable(0), new IntWritable(0)), p);
		}
		
		// random first k point center, write them to sequence file pathCenter
		writer = SequenceFile.createWriter(conf, Writer.file(new Path(pathCenter)), 
		        Writer.keyClass(Point.class), Writer.valueClass(Point.class));
		Random rand = new Random();
		int numberOfPoint = listPoints.size();
		for(int i=0; i<k; i++) {
			int tmp = rand.nextInt(numberOfPoint);
			Point tmpPoint = listPoints.get(tmp);
			writer.append(tmpPoint, new Point(new IntWritable(0), new IntWritable(0)));
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
		kmeans.convertTextToSequenceFile(conf, in, cen);
		
		conf.set("pathIn", in);
		conf.set("pathCenter", cen);
		conf.set("pathOut", out);

		Job job = Job.getInstance(conf, "K-Means");
		job.setMapperClass(KmeansMap.class);
		job.setReducerClass(KmeansReduce.class);
		job.setJarByClass(Kmeans.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setOutputKeyClass(Point.class);
		job.setOutputValueClass(Point.class);
		
		FileInputFormat.addInputPath(job, new Path(in));
		FileSystem fs = FileSystem.get(conf); // delete file output when it exists
		if (fs.exists(new Path(out))) {
			fs.delete(new Path(out), true);
		}
		
		FileOutputFormat.setOutputPath(job, new Path(out));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
