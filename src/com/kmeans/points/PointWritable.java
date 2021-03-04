package com.kmeans.points;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class PointWritable implements WritableComparable<PointWritable> {
	private IntWritable x;
	private IntWritable y;

	public PointWritable() {
		this(new IntWritable(0), new IntWritable(0));
	}

	public PointWritable(IntWritable x, IntWritable y) {
		setX(x);
		setY(y);
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

	@Override
	public String toString() {
		return "{" + x.get() + "," + y.get() + "}";
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
		
        PointWritable pw = (PointWritable) obj;
		return Objects.equals(x, pw.getX()) && Objects.equals(y, pw.getY());
	}

	@Override
	public int hashCode() {
		return Objects.hash(x, y);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		x.write(out);
		y.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		x.readFields(in);
		y.readFields(in);
	}

	@Override
	public int compareTo(PointWritable o) {
		if(o == null) {
			return -1;
		}
		
		return 0;
	}
}
