package com.kmeans.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Mapper;

import com.kmeans.distance.Distance;
import com.kmeans.distance.EuclideanDistance;
import com.kmeans.points.PointWritable;

public class KmeansMap extends Mapper<PointWritable, PointWritable, PointWritable, PointWritable> {
	private static List<PointWritable> listCenter = new ArrayList<>();

	/*
	 * Method setup, we will get k centers from sequence file cen.seq
	 */
	@SuppressWarnings("resource")
	@Override
	protected void setup(Mapper<PointWritable, PointWritable, PointWritable, PointWritable>.Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Path centers = new Path(conf.get("pathCenter"));
		
		SequenceFile.Reader reader = null;
		reader = new SequenceFile.Reader(conf, Reader.file(centers));
		
		PointWritable key = new PointWritable();
		PointWritable value = new PointWritable();
		while(reader.next(key, value)) {
			listCenter.add(key);
		}		
		
		reader.close();
	}

	@Override
	protected void map(PointWritable key, PointWritable value,
			Mapper<PointWritable, PointWritable, PointWritable, PointWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.print("number of center : " + listCenter.size() + " => ");
		System.out.print("mapper : ");
		
		PointWritable nearest = null;
		Distance distance = new EuclideanDistance();
		double minDistance = Double.MAX_VALUE;
		
		for(PointWritable c : listCenter) {
			double tmpDistance = distance.calculate(c, value);
			if(tmpDistance < minDistance) {
				minDistance = tmpDistance;
				nearest = c;
			}
		}
		context.write(nearest, value);
		
		System.out.print("(center)" + nearest + " | (point)" + value + "\n");

	}
}
