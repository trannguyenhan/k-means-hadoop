package com.kmeans.distance;

import com.kmeans.points.PointWritable;

public interface Distance {
	double calculate(PointWritable p1, PointWritable p2);
}
