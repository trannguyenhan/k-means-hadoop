package com.kmeans.distance;

import com.kmeans.points.Point;

public class EuclideanDistance implements Distance {

	@Override
	public double calculate(Point p1, Point p2) {
		return Math.sqrt((p1.getX().get() - p2.getX().get()) * (p1.getX().get() - p2.getX().get())
				+ (p1.getY().get() - p2.getY().get()) * (p1.getY().get() - p2.getY().get()));
	}
}
