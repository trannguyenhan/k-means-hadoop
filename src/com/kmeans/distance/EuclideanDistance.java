package com.kmeans.distance;

import com.kmeans.points.PointWritable;

public class EuclideanDistance implements Distance {
	@Override
	public double calculate(PointWritable p1, PointWritable p2) {
		return Math
				.sqrt(Math.pow(p1.getX().get() - p2.getX().get(), 2) + Math.pow(p1.getY().get() - p2.getY().get(), 2));
	}
}
