package com.tpmms.service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.tpmms.buffers.Buffer;
import com.tpmms.utility.CommonUtility;
import com.tpmms.utility.FileConstants;
import com.tpmms.utility.QuickSort;
import com.tpmms.utility.SizeConstants;

public class TpmmsService {
	
	final StringBuilder sublistFilePath;
	final int totalBlocks;
	final int totalFills;
	final int mainMemoryBlocks;
	final File file;
	final int fileSize;
	final StringBuilder fileName;
	ByteBuffer outputBuffer ;
	Buffer[] sublistBufferArray ;
	Buffer[] bagBufferArray ;
	int sortReadCount;
	int sortWriteCount;
	int mergeReadCount;
	int mergeWriteCount;
	int bagDiffReadCount;
	int nonEmptyBufferListSize ;
	
	public TpmmsService(final String filePath) {
		sublistFilePath = new StringBuilder(FileConstants.SUBLISTS_FILE_PATH);
		sublistFilePath.append(FileConstants.SUBLIST_FILE_NAME);
		file = new File(filePath);
		fileName = new StringBuilder(file.getName().substring(0, file.getName().lastIndexOf(".")));
		fileSize = (int)file.length();
		System.out.println("Total Number of Tuples : " + fileSize / SizeConstants.TUPLE_SIZE);
		totalBlocks = CommonUtility.getTotalBlocks(fileSize, SizeConstants.BLOCK_SIZE);
		System.out.println("Total Number of Blocks : " + totalBlocks);
		totalFills = CommonUtility.getTotalFills(fileSize, SizeConstants.MAIN_MEMORY_SIZE);
		System.out.println("Total Main Memory Fills : " + totalFills);
		mainMemoryBlocks = CommonUtility.getMainMemoryBlocks(SizeConstants.MAIN_MEMORY_SIZE, SizeConstants.BLOCK_SIZE);
		System.out.println("Total Main Memory Blocks : " + mainMemoryBlocks);
		System.out.println("-------------------------------------");
	}

	public int tpmmsSort() {
		
		try {
			final FileChannel inputFileChannel = FileChannel.open(Paths.get(file.getPath()), EnumSet.of(StandardOpenOption.READ));
			final ByteBuffer inputBuffer = ByteBuffer.allocate(SizeConstants.MAIN_MEMORY_SIZE);
			final FileService fileService =  new FileService();
			
			for(int run = 0; run < totalFills-1; run++) {
				fileService.readInputFile(inputFileChannel, inputBuffer);
				QuickSort.sort(0, SizeConstants.MAIN_MEMORY_SIZE - SizeConstants.TUPLE_SIZE, inputBuffer);
				write(inputBuffer, fileService, run) ;
			}
			
			if (fileSize % SizeConstants.MAIN_MEMORY_SIZE != 0) {
				fileService.readInputFile(inputFileChannel, inputBuffer);
				QuickSort.sort(0, ((fileSize % SizeConstants.MAIN_MEMORY_SIZE) / SizeConstants.TUPLE_SIZE * SizeConstants.TUPLE_SIZE) - SizeConstants.TUPLE_SIZE, inputBuffer);
				write(inputBuffer, fileService, totalFills - 1);
			} else {
				fileService.readInputFile(inputFileChannel, inputBuffer);
				QuickSort.sort(0, SizeConstants.MAIN_MEMORY_SIZE - SizeConstants.TUPLE_SIZE, inputBuffer);
				write(inputBuffer, fileService, totalFills - 1);
			}

			if (inputFileChannel.isOpen()) {
				try {
					inputFileChannel.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			sortReadCount = fileService.getReadCount();
			sortWriteCount = fileService.getWriteCount();
			System.out.println(fileName + " - Total Blocks Read : " + sortReadCount);
			System.out.println(fileName + " - Total Blocks Write : " + sortWriteCount);
		} catch (FileNotFoundException fnfException) {
			System.err.println("File not exits: " + fnfException.getMessage());
			return 0;
		} catch (IOException ioException) {
			System.err.println("Erro while reading: " + ioException.getMessage());
			return 0;
		}

		return sortReadCount + sortWriteCount;
	}
	
	private void write(final ByteBuffer inputBuffer, final FileService fileService, final int run) throws FileNotFoundException, IOException {
		final String sublistFileName = sublistFilePath.toString() + fileName + "_" + run + ".txt";
		final FileChannel outputFileChannel = FileChannel.open(Files.createFile(Paths.get(sublistFileName)), EnumSet.of(StandardOpenOption.WRITE));
		fileService.writeOutputFile(outputFileChannel, inputBuffer);
		if (outputFileChannel.isOpen()) {
			try {
				outputFileChannel.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public int tpmmsMerge() {
		try {
			final FileChannel outputChannel = FileChannel.open(Files.createFile(Paths.get(FileConstants.OUTPUT_FILE_PATH + fileName + "_sorted.txt")),
																EnumSet.of(StandardOpenOption.APPEND));
			fillSublistBuffers();

			final int outputBufferTuples = outputBuffer.capacity() / SizeConstants.TUPLE_SIZE;
			int outputBufferRefills = CommonUtility.getTotalNoOfTuples(fileSize, SizeConstants.TUPLE_SIZE) / outputBufferTuples;

			arrangeSublistBuffers();

			while (outputBufferRefills != 0) {
				int i = outputBufferTuples;
				while (i != 0) {
					outputBuffer.put(getSmallestTuple());
					--i;
				}
				writeOutputBuffer(outputChannel);
				--outputBufferRefills;
			}

			int j = CommonUtility.getTotalNoOfTuples(fileSize, SizeConstants.TUPLE_SIZE) % outputBufferTuples;
			while (j != 0) {
				outputBuffer.put(getSmallestTuple());
				--j;
			}
			writeOutputBuffer(outputChannel);

			if (outputChannel.isOpen()) {
				try {
					outputChannel.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			for (int i = 0; i != sublistBufferArray.length; i++) {
				sublistBufferArray[i].setBuffer(null);
				if (sublistBufferArray[i].getFileChannel().isOpen()) {
					try {
						sublistBufferArray[i].getFileChannel().close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			sublistBufferArray = null;
			outputBuffer = null;
			
			System.out.println(fileName + " - Total Blocks Read : " + mergeReadCount);
			System.out.println(fileName + " - Total Blocks Write : " + mergeWriteCount);
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
		return mergeReadCount + mergeWriteCount;
	}
	
	private void fillSublistBuffers() {
		try {
			// Calculate SublistBuffer Size and Output buffer size
			final int sublistBufferSize = (int) ((mainMemoryBlocks * 0.7) / totalFills) * SizeConstants.BLOCK_SIZE;
			final int outputBufferSize = (int) ((mainMemoryBlocks * 0.3) + ((mainMemoryBlocks * 0.7) % totalFills)) * SizeConstants.BLOCK_SIZE;

			sublistBufferArray = new Buffer[totalFills];

			// Create sublist buffers and perform first read.
			for (int i = 0; i < totalFills-1; i++) {
				final String sublistFileName = sublistFilePath.toString() + fileName + "_" + i + ".txt";
				final FileChannel fileChannel = FileChannel.open(Paths.get(sublistFileName), EnumSet.of(StandardOpenOption.READ));
				sublistBufferArray[i] = new Buffer(ByteBuffer.allocate(sublistBufferSize), 0, SizeConstants.MAIN_MEMORY_SIZE - 1, fileChannel);
				sublistBufferArray[i].readBuffer();
				final int bytesRead = sublistBufferArray[i].getBuffer().limit() - sublistBufferArray[i].getBuffer().position();
				mergeReadCount += bytesRead / SizeConstants.BLOCK_SIZE;
				if(bytesRead % SizeConstants.BLOCK_SIZE != 0) {
					mergeReadCount++;
				}
			}

			// Create last sublist buffer. (It can be smaller than others)
			final String sublistFileName = sublistFilePath.toString() + fileName + "_" + (totalFills - 1) + ".txt";
			final FileChannel fileChannel = FileChannel.open(Paths.get(sublistFileName), EnumSet.of(StandardOpenOption.READ));
			sublistBufferArray[totalFills - 1] = new Buffer(ByteBuffer.allocate(sublistBufferSize), 0, new File(sublistFileName).length() - 1, fileChannel);
			sublistBufferArray[totalFills - 1].readBuffer();
			final int bytesRead = sublistBufferArray[totalFills - 1].getBuffer().limit()	- sublistBufferArray[totalFills - 1].getBuffer().position();
			mergeReadCount += bytesRead / SizeConstants.BLOCK_SIZE;
			if(bytesRead % SizeConstants.BLOCK_SIZE != 0) {
				mergeReadCount++;
			}
			
			// Create output buffer
			outputBuffer = ByteBuffer.allocate(outputBufferSize);

		} catch (IOException e) {
			System.out.println("The following error occurred during initial sublist buffer load: " + e.getMessage());
		}
	}
	
	private void arrangeSublistBuffers() {
		nonEmptyBufferListSize = sublistBufferArray.length - 1;
		for (int i = (sublistBufferArray.length / 2) - 1; i != -1; --i) {
			findSmallestSublistBuffer(i);
		}
	}

	private void findSmallestSublistBuffer(int i) {
		final int leftChild = (2 * i) + 1;
		final int rightChild = ((2 * i) + 1) + 1;
		int smallest;
		if (leftChild <= nonEmptyBufferListSize && sublistBufferArray[leftChild].compareTo(sublistBufferArray[i]) < 0) {
			smallest = leftChild;
		} else {
			smallest = i;
		}
		if (rightChild <= nonEmptyBufferListSize && sublistBufferArray[rightChild].compareTo(sublistBufferArray[smallest]) < 0) {
			smallest = rightChild;
		}
		if (smallest != i) {
			final Buffer tempBuffer = sublistBufferArray[smallest];
			sublistBufferArray[smallest] = sublistBufferArray[i];
			sublistBufferArray[i] = tempBuffer;
			findSmallestSublistBuffer(smallest);
		}
	}

	private byte[] getSmallestTuple() {
		final byte[] tuple = new byte[SizeConstants.TUPLE_SIZE];
		try {
			sublistBufferArray[0].getBuffer().get(tuple);
		} catch (BufferUnderflowException e) {
		}

		if (sublistBufferArray[0].getBuffer().position() == sublistBufferArray[0].getBuffer().limit()) {
			if (!sublistBufferArray[0].readBuffer()) {
				final Buffer tempBuffer = sublistBufferArray[0];
				sublistBufferArray[0] = sublistBufferArray[nonEmptyBufferListSize];
				sublistBufferArray[nonEmptyBufferListSize] = tempBuffer;
				--nonEmptyBufferListSize;
			} else {
				final int bytesRead = sublistBufferArray[0].getBuffer().limit() - sublistBufferArray[0].getBuffer().position();
				mergeReadCount += bytesRead / SizeConstants.BLOCK_SIZE;
				if(bytesRead % SizeConstants.BLOCK_SIZE != 0) {
					mergeReadCount++;
				}
			}
		}

		findSmallestSublistBuffer(0);
		return tuple;
	}
	
	private void writeOutputBuffer(final FileChannel outputChannel) {
		outputBuffer.flip();
		try {
			final int bytesWritten = outputChannel.write(outputBuffer);
			mergeWriteCount += bytesWritten / SizeConstants.BLOCK_SIZE;
			if(bytesWritten % SizeConstants.BLOCK_SIZE != 0) {
				mergeWriteCount++;
			}
			if (outputBuffer.position() != outputBuffer.limit()) {
				throw new IOException("Output Buffer write not successfull");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		outputBuffer.clear();
	}
	
	public int getBagDifference() {
		try {
			final FileChannel outputChannel = FileChannel.open(Files.createFile(Paths.get(FileConstants.OUTPUT_FILE_PATH + FileConstants.BAG_DIFF_FILE_NAME)),
																EnumSet.of(StandardOpenOption.APPEND));
			final File t1SortedRelation = new File(FileConstants.OUTPUT_FILE_PATH + FileConstants.RELATION_T1.substring(0, FileConstants.RELATION_T1.lastIndexOf(".")) + "_sorted.txt");
			final File t2SortedRelation = new File(FileConstants.OUTPUT_FILE_PATH + FileConstants.RELATION_T2.substring(0, FileConstants.RELATION_T2.lastIndexOf(".")) + "_sorted.txt");
			
			fillBagBuffers(t1SortedRelation, t2SortedRelation);
			
			final int outputBufferTuples = outputBuffer.capacity() / SizeConstants.TUPLE_SIZE;
			int outputBufferRefills = CommonUtility.getTotalNoOfTuples((int)t1SortedRelation.length(), SizeConstants.TUPLE_SIZE) / outputBufferTuples;
			
			int i;
			boolean isT1BagReadComplete = false;
			boolean isT2BagReadComplete = false;
			
			while (outputBufferRefills != 0) {
				i = outputBufferTuples;
				while (i != 0) {
					int result = bagBufferArray[0].compareTo(bagBufferArray[1]);

					if (result == -1) {
						byte[] tuple = new byte[SizeConstants.TUPLE_SIZE];
						bagBufferArray[0].getBuffer().get(tuple);
						outputBuffer.put(tuple);
						--i;
					} else if (result == 1) {
						bagBufferArray[1].getBuffer().position(bagBufferArray[1].getBuffer().position() + SizeConstants.TUPLE_SIZE);
					} else {
						bagBufferArray[0].getBuffer().position(bagBufferArray[0].getBuffer().position() + SizeConstants.TUPLE_SIZE);
						bagBufferArray[1].getBuffer().position(bagBufferArray[1].getBuffer().position() + SizeConstants.TUPLE_SIZE);
					}

					boolean isBagBufferRefillSuccessful = refillBgaBuffer(bagBufferArray[0]);
					if(!isBagBufferRefillSuccessful) {
						isT1BagReadComplete = true;
						break;
					}
					
					isBagBufferRefillSuccessful = refillBgaBuffer(bagBufferArray[1]);
					if(!isBagBufferRefillSuccessful) {
						isT2BagReadComplete = true;
						break;
					}
				}
				
				writeOutputBuffer(outputChannel);
				if (isT1BagReadComplete || isT2BagReadComplete) {
					break;
				}
			}
			
			if (!isT1BagReadComplete && isT2BagReadComplete) {
				final int remainingTuples = (int) ((bagBufferArray[0].getLastPosition() - bagBufferArray[0].getCurrentPosition() + 1 + 
													bagBufferArray[0].getBuffer().limit() - bagBufferArray[0].getBuffer().position()) / SizeConstants.TUPLE_SIZE);
				outputBufferRefills = remainingTuples / outputBufferTuples;
				
				if(remainingTuples % outputBufferTuples > 0) {
					outputBufferRefills++;
				}
				
				while (outputBufferRefills != 0) {
					i = outputBufferTuples;
					while (i != 0) {
						byte[] tuple = new byte[SizeConstants.TUPLE_SIZE];
						bagBufferArray[0].getBuffer().get(tuple);
						outputBuffer.put(tuple);
						--i;
						
						boolean isBagBufferRefillSuccessful = refillBgaBuffer(bagBufferArray[0]);
						if(!isBagBufferRefillSuccessful) {
							isT1BagReadComplete = true;
							break;
						}
					}
					writeOutputBuffer(outputChannel);
					if (isT1BagReadComplete) {
						break;
					}
				}
				
			}
			
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
		return bagDiffReadCount;
	}
	
	public int getBagDifferenceCount() {
		try {
			final File t1SortedRelation = new File(FileConstants.OUTPUT_FILE_PATH + FileConstants.RELATION_T1.substring(0, FileConstants.RELATION_T1.lastIndexOf(".")) + "_sorted.txt");
			final File t2SortedRelation = new File(FileConstants.OUTPUT_FILE_PATH + FileConstants.RELATION_T2.substring(0, FileConstants.RELATION_T2.lastIndexOf(".")) + "_sorted.txt");
			
			fillBagBuffers(t1SortedRelation, t2SortedRelation);
			
			final int outputBufferSize = (int) ((mainMemoryBlocks * 0.3) + ((mainMemoryBlocks * 0.7) % 2)) * SizeConstants.BLOCK_SIZE;
			final int outputBufferTuples = outputBufferSize / SizeConstants.TUPLE_SIZE;
			int outputBufferRefills = CommonUtility.getTotalNoOfTuples((int)t1SortedRelation.length(), SizeConstants.TUPLE_SIZE) / outputBufferTuples;
			
			int i;
			boolean isT1BagReadComplete = false;
			boolean isT2BagReadComplete = false;
			
			final HashMap<String, Integer> bagDiff = new HashMap<>();
			final BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File(FileConstants.OUTPUT_FILE_PATH + FileConstants.BAG_DIFF_COUNT_FILE_NAME)));
			
			while (outputBufferRefills != 0) {
				i = outputBufferTuples;
				while (i != 0) {
					int result = bagBufferArray[0].compareTo(bagBufferArray[1]);
					
					if (result == -1) {
						byte[] tuple = new byte[SizeConstants.TUPLE_SIZE];
						bagBufferArray[0].getBuffer().get(tuple);
						final String strTuple = new String(tuple);
						if(bagDiff.containsKey(strTuple)) {
							bagDiff.put(strTuple, bagDiff.get(strTuple)+1);
						}else {
							bagDiff.put(strTuple, 1);
						}
						--i;
					} else if (result == 1) {
						bagBufferArray[1].getBuffer().position(bagBufferArray[1].getBuffer().position() + SizeConstants.TUPLE_SIZE);
						final Iterator<Map.Entry<String, Integer>> iterator = bagDiff.entrySet().iterator();
						while (iterator.hasNext()) {
						    final Map.Entry<String, Integer> entry = iterator.next();
						    //System.out.println(entry.getKey() + " : " + entry.getValue());
						    bufferedWriter.write(entry.getKey().replace("\n", "") + " : " + entry.getValue());
							bufferedWriter.newLine();
							iterator.remove();
						}
					} else {
						bagBufferArray[0].getBuffer().position(bagBufferArray[0].getBuffer().position() + SizeConstants.TUPLE_SIZE);
						bagBufferArray[1].getBuffer().position(bagBufferArray[1].getBuffer().position() + SizeConstants.TUPLE_SIZE);
						final Iterator<Map.Entry<String, Integer>> iterator = bagDiff.entrySet().iterator();
						while (iterator.hasNext()) {
							final Map.Entry<String, Integer> entry = iterator.next();
							//System.out.println(entry.getKey() + " : " + entry.getValue());
							bufferedWriter.write(entry.getKey().replace("\n", "") + " : " + entry.getValue());
							bufferedWriter.newLine();
							iterator.remove();
						}
					}
					
					boolean isBagBufferRefillSuccessful = refillBgaBuffer(bagBufferArray[0]);
					if(!isBagBufferRefillSuccessful) {
						isT1BagReadComplete = true;
						break;
					}
					
					isBagBufferRefillSuccessful = refillBgaBuffer(bagBufferArray[1]);
					if(!isBagBufferRefillSuccessful) {
						isT2BagReadComplete = true;
						break;
					}
				}
				
				if (isT1BagReadComplete || isT2BagReadComplete) {
					break;
				}
			}
			
			if (!isT1BagReadComplete && isT2BagReadComplete) {
				final int remainingTuples = (int) ((bagBufferArray[0].getLastPosition() - bagBufferArray[0].getCurrentPosition() + 1 + 
													bagBufferArray[0].getBuffer().limit() - bagBufferArray[0].getBuffer().position()) / SizeConstants.TUPLE_SIZE);
				outputBufferRefills = remainingTuples / outputBufferTuples;
				
				if(remainingTuples % outputBufferTuples > 0) {
					outputBufferRefills++;
				}
				
				while (outputBufferRefills != 0) {
					i = outputBufferTuples;
					while (i != 0) {
						byte[] tuple = new byte[SizeConstants.TUPLE_SIZE];
						bagBufferArray[0].getBuffer().get(tuple);
						final String strTuple = new String(tuple);
						if(bagDiff.containsKey(strTuple)) {
							bagDiff.put(strTuple, bagDiff.get(strTuple)+1);
						}else {
							bagDiff.put(strTuple, 1);
						}
						--i;
						
						boolean isBagBufferRefillSuccessful = refillBgaBuffer(bagBufferArray[0]);
						if(!isBagBufferRefillSuccessful) {
							isT1BagReadComplete = true;
							break;
						}
					}
					final Iterator<Map.Entry<String, Integer>> iterator = bagDiff.entrySet().iterator();
					while (iterator.hasNext()) {
					    final Map.Entry<String, Integer> entry = iterator.next();
					    //System.out.println(entry.getKey() + " : " + entry.getValue());
					    bufferedWriter.write(entry.getKey().replace("\n", "") + " : " + entry.getValue());
						bufferedWriter.newLine();
						iterator.remove();
					}
					if (isT1BagReadComplete) {
						break;
					}
				}
				
			}
			bufferedWriter.close();
			
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
		return bagDiffReadCount;
	}
	
	private void fillBagBuffers(final File t1, final File t2) {
		try {
			// Calculate BagBuffer Size and Output buffer size
			final int bagBufferSize = (int) ((mainMemoryBlocks * 0.7) / 2) * SizeConstants.BLOCK_SIZE;
			//final int outputBufferSize = (int) ((mainMemoryBlocks * 0.3) + ((mainMemoryBlocks * 0.7) % 2)) * SizeConstants.BLOCK_SIZE;
			
			bagBufferArray = new Buffer[2];
			
			//Create T1 Bag Buffer.
			final FileChannel t1SortedFileChannel = FileChannel.open(Paths.get(t1.getPath()), EnumSet.of(StandardOpenOption.READ));
			bagBufferArray[0] = new Buffer(ByteBuffer.allocate(bagBufferSize), 0, t1.length() -1, t1SortedFileChannel);
			bagBufferArray[0].readBuffer();

			//Create T2 Bag Buffer.
			final FileChannel t2SortedFileChannel = FileChannel.open(Paths.get(t2.getPath()), EnumSet.of(StandardOpenOption.READ));
			bagBufferArray[1] = new Buffer(ByteBuffer.allocate(bagBufferSize), 0, t2.length() -1, t2SortedFileChannel);
			bagBufferArray[1].readBuffer();
			
			//Calculate initial total read count.
			int bytesRead = bagBufferArray[0].getBuffer().limit() - bagBufferArray[0].getBuffer().position();
			bagDiffReadCount += bytesRead / SizeConstants.BLOCK_SIZE;
			if(bytesRead % SizeConstants.BLOCK_SIZE != 0) {
				bagDiffReadCount++;
			}

			bytesRead = bagBufferArray[1].getBuffer().limit() - bagBufferArray[1].getBuffer().position();
			bagDiffReadCount += bytesRead / SizeConstants.BLOCK_SIZE;
			if(bytesRead % SizeConstants.BLOCK_SIZE != 0) {
				bagDiffReadCount++;
			}
			
			// Create output buffer
			//outputBuffer = ByteBuffer.allocate(outputBufferSize);
			
		} catch (IOException e) {
			System.out.println("The following error occurred during initial bag buffer load: " + e.getMessage());
		}
	}
	
	private boolean refillBgaBuffer(final Buffer bagBuffer) {
		boolean isRefillSuccessful = true;
		if (bagBuffer.getBuffer().position() == bagBuffer.getBuffer().limit()) {
			if (!bagBuffer.readBuffer()) {
				isRefillSuccessful = false;
			} else {
				final int bytesRead = bagBuffer.getBuffer().limit() - bagBuffer.getBuffer().position();
				bagDiffReadCount += bytesRead / SizeConstants.BLOCK_SIZE;
				if(bytesRead % SizeConstants.BLOCK_SIZE != 0) {
					bagDiffReadCount++;
				}
			}
		}
		return isRefillSuccessful;
	}
	
}