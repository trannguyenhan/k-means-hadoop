package com.kmeans.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Reducer;

import com.kmeans.distance.Distance;
import com.kmeans.distance.EuclideanDistance;
import com.kmeans.points.Point;

public class KmeansReduce extends Reducer<Point, Point, Point, Point>{
	private static List<Point> newListCenter = new ArrayList<>();
	
	@Override
	protected void reduce(Point key, Iterable<Point> values, Reducer<Point, Point, Point, Point>.Context context)
			throws IOException, InterruptedException {
		System.out.println("reducer running...");
		
		List<Point> list = new ArrayList<>();
		for(Point p : values) {
			list.add(p);
		}
		
		Point newCenter = createNewCenter(list);
		newListCenter.add(newCenter);
		
		for(Point p : list) {
			context.write(newCenter, p);
		}
	}
	
	@Override
	protected void cleanup(Reducer<Point, Point, Point, Point>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String pathCenter = conf.get("pathCenter");
		
		SequenceFile.Writer writer = null;
		writer = SequenceFile.createWriter(conf, Writer.file(new Path(pathCenter)), 
		        Writer.keyClass(Point.class), Writer.valueClass(Point.class));
		
		for(Point c : newListCenter) {
			writer.append(c, new Point(new IntWritable(0), new IntWritable(0)));
		}
		
		writer.close();
	}
	
	/* Create new center from list point
	 * */
	public Point createNewCenter(List<Point> list) {
		Distance distance = new EuclideanDistance();
		
		Point avg = avgPoint(list);
		Point nearest = null;
		double minDistance = Double.MAX_VALUE;
		
		for(Point p : list) {
			double tmpDistance = distance.calculate(p, avg);
			if(tmpDistance < minDistance) {
				minDistance = tmpDistance;
				nearest = p;
			}
		}
		
		return nearest;
	}
	
	public Point avgPoint(List<Point> list) {
		int numberOfPoint = list.size();
		int x = 0;
		int y = 0;
		
		for(Point p : list) {
			x += p.getX().get();
			y += p.getY().get();
		}
		
		x = x / numberOfPoint;
		y = y / numberOfPoint;
		
		return new Point(new IntWritable(x), new IntWritable(y));
	}
}
