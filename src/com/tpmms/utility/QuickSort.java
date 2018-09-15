package com.tpmms.utility;

import java.nio.ByteBuffer;

public class QuickSort {

	public static void sort(final int left, final int right, final ByteBuffer outputBuffer) {
		if( left < right) {
			final int r = partition(left, right, outputBuffer) ;
			sort( left, r - SizeConstants.TUPLE_SIZE, outputBuffer) ;
			sort( r + SizeConstants.TUPLE_SIZE, right, outputBuffer) ;
		}
	}

	private static int partition(final int left, final int right, final ByteBuffer outputBuffer) {
		final int pivot = right;
		int i = left - SizeConstants.TUPLE_SIZE;
		for (int j = left; j < right; j = j + SizeConstants.TUPLE_SIZE) {
			for (int k = 0; k != SizeConstants.TUPLE_SIZE; ++k) {
				if (outputBuffer.get(j+k) < outputBuffer.get(pivot + k)) {
					i = i + SizeConstants.TUPLE_SIZE;
					exchange(i, j, outputBuffer);
					break;
				} else if (outputBuffer.get(j + k) > outputBuffer.get(pivot + k)) {
					break;
				}
			}
		}
		i = i + SizeConstants.TUPLE_SIZE;
		exchange(i, right, outputBuffer);
		return i;
	}
	
	private static void exchange(final int i, final int j, final ByteBuffer outputBuffer) {
		if (i == j) {
			return;
		}
		
		final int currentPosition = outputBuffer.position() ;
		
		outputBuffer.position(i) ;
		final byte[] tempI = new byte[SizeConstants.TUPLE_SIZE] ;
		outputBuffer.get(tempI) ;
		
		outputBuffer.position(j) ;
		final byte[] tempJ = new byte[SizeConstants.TUPLE_SIZE] ;
		outputBuffer.get(tempJ) ;
		
		outputBuffer.position(j) ;
		outputBuffer.put(tempI) ;
		
		outputBuffer.position(i) ;
		outputBuffer.put(tempJ) ;
		
		outputBuffer.position(currentPosition) ;
	}
	
}
