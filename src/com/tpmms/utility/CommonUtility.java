package com.tpmms.utility;

public class CommonUtility {

	public static int getTotalNoOfTuples(final int fileSize, final int tupleSize) {
		return fileSize / tupleSize;
	}

	public static long getNoOfTuplesPerBlock(final long blockSize, final int tupleSize) {
		return blockSize / tupleSize;
	}

	public static int getTotalBlocks(final int fileSize, final int blockSize) {
		return (int) Math.ceil((double) fileSize / blockSize);
	}

	public static int getMainMemoryBlocks(final int mainMemorySize, final int blockSize) {
		return mainMemorySize / blockSize;
	}

	public static int getTotalFills(final int fileSize, final int mainMemorySize) {
		return (int) Math.ceil((double) fileSize / mainMemorySize);
	}

	public static int getMemorySizeInMB() {
		return (int) (Runtime.getRuntime().totalMemory() / (1024 * 1024));
	}
}
