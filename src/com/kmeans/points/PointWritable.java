package com.kmeans.points;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class PointWritable implements WritableComparable<PointWritable> {
	private IntWritable x;
	private IntWritable y;
	private DoubleWritable distanceForCentroid;

	public PointWritable() {
		this(0, 0);
	}

	public PointWritable(IntWritable x, IntWritable y) {
		this(x.get(), y.get());
	}

	public PointWritable(int x, int y) {
		setX(new IntWritable(x));
		setY(new IntWritable(y));

		double distance = calcDistanceForCentroid(x, y);
		distanceForCentroid = new DoubleWritable(distance);
	}

	public double calcDistanceForCentroid(int x, int y) {
		return Math.sqrt(x * x + y * y);
	}

	public IntWritable getX() {
		return x;
	}

	public void setX(IntWritable x) {
		this.x = x;
	}

	public IntWritable getY() {
		return y;
	}

	public void setY(IntWritable y) {
		this.y = y;
	}

	public DoubleWritable getDistanceForCentroid() {
		return distanceForCentroid;
	}

	public void setDistanceForCentroids(DoubleWritable distanceForCentroid) {
		this.distanceForCentroid = distanceForCentroid;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		x.readFields(in);
		y.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		x.write(out);
		y.write(out);
	}

	@Override
	public int compareTo(PointWritable o) {
		double distD = this.getDistanceForCentroid().get() - o.getDistanceForCentroid().get();
		int distI = (int) Math.round(distD);

		return distI;
	}

	@Override
	public String toString() {
		return x.get() + " " + y.get();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}

		if (!(obj instanceof PointWritable)) {
			return false;
		}

		return x.get() == ((PointWritable) obj).getX().get() && y.get() == ((PointWritable) obj).getY().get();
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(x.get(), y.get());
	}
}
