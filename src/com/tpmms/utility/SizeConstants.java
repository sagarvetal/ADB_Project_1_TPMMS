package com.tpmms.utility;

public class SizeConstants {

	public static final int TUPLE_SIZE = 100;
	public static final int BLOCK_SIZE = (4096 / TUPLE_SIZE) * TUPLE_SIZE;
	public static final int MAIN_MEMORY_SIZE = (((int)(Runtime.getRuntime().totalMemory() * 0.5) / BLOCK_SIZE) * BLOCK_SIZE);
}
