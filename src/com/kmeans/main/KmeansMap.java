package com.kmeans.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.kmeans.distance.Distance;
import com.kmeans.distance.EuclideanDistance;
import com.kmeans.points.PointService;
import com.kmeans.points.PointWritable;

public class KmeansMap extends Mapper<Object, Text, PointWritable, PointWritable> {
	Distance distance;
	private static List<PointWritable> listCenter = new ArrayList<>();

	/*
	 * Method setup, we will get k centers from sequence file cen.seq
	 */
	@SuppressWarnings("resource")
	@Override
	protected void setup(Mapper<Object, Text, PointWritable, PointWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println("setup running...");
		
		Configuration conf = context.getConfiguration();
		
		String pathCenter = conf.get("pathCenter");
		listCenter = PointService.getListPoints(pathCenter);
		
		distance = new EuclideanDistance();
		
		System.out.println("setup close...");
	}

	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, PointWritable, PointWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println("mapper running...");
		
		StringTokenizer token = new StringTokenizer(value.toString(), "\n");
		while(token.hasMoreTokens()) {
			String line = token.nextToken();
			String[] xy = line.split(" ");
			
			int x = Integer.parseInt(xy[0]);
			int y = Integer.parseInt(xy[1]);
			PointWritable pw = new PointWritable(x, y);
			
			double minDistance = Double.MAX_VALUE;
			PointWritable nearest = null;
			for(PointWritable c : listCenter) {
				double tmpDistance = distance.calculate(pw, c);
				if(tmpDistance < minDistance) {
					nearest = c;
					minDistance = tmpDistance;
				}
			}
			
			context.write(nearest, pw);
			System.out.println("mapper : " + nearest + " / " + pw);
		}
		
		System.out.println("mapper close...");
	}
}
