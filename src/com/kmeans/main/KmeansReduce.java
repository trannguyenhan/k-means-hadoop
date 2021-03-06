package com.kmeans.main;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.kmeans.distance.Distance;
import com.kmeans.distance.EuclideanDistance;
import com.kmeans.points.PointWritable;

public class KmeansReduce extends Reducer<PointWritable, PointWritable, PointWritable, PointWritable> {
	private static List<PointWritable> newListCenter = new ArrayList<>();

	@Override
	protected void reduce(PointWritable key, Iterable<PointWritable> values,
			Reducer<PointWritable, PointWritable, PointWritable, PointWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println("reducer running...");
		System.out.println("output map : " + key + " " + values);
		List<PointWritable> list = new ArrayList<>();
		for (PointWritable p : values) {
			list.add(p);
		}

		PointWritable newCenter = createNewCenter(list);
		newListCenter.add(newCenter);

		for (PointWritable p : list) {
			context.write(newCenter, p);
			System.out.println("reducer : " + newCenter + " " + p);
		}

		System.out.println("reducer close...");
	}

	@Override
	protected void cleanup(Reducer<PointWritable, PointWritable, PointWritable, PointWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println("cleanup running...");
		
		Configuration conf = context.getConfiguration();
		String pathCenter = conf.get("pathCenter");
		
		try (PrintWriter printWriter = new PrintWriter(pathCenter)) {
			for(PointWritable c : newListCenter) {
				printWriter.write(c + "\n");
				System.out.println("center : " + c);
			}
		}
		
		System.out.println("cleanup close...");
	}

	/*
	 * Create new center from list point
	 */
	public PointWritable createNewCenter(List<PointWritable> list) {
		Distance distance = new EuclideanDistance();

		PointWritable avg = avgPoint(list);
		PointWritable nearest = null;
		double minDistance = Double.MAX_VALUE;

		for (PointWritable p : list) {
			double tmpDistance = distance.calculate(p, avg);
			if (tmpDistance < minDistance) {
				minDistance = tmpDistance;
				nearest = p;
			}
		}

		return nearest;
	}

	public PointWritable avgPoint(List<PointWritable> list) {
		int numberOfPoint = list.size();
		int x = 0;
		int y = 0;

		for (PointWritable p : list) {
			x += p.getX().get();
			y += p.getY().get();
		}

		x = x / numberOfPoint;
		y = y / numberOfPoint;

		return new PointWritable(new IntWritable(x), new IntWritable(y));
	}
}
