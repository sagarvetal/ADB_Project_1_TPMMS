package com.tpmms.service;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.tpmms.utility.SizeConstants;

public class FileService {
	
	private int readCount;
	private int writeCount;

	public void readInputFile(final FileChannel fileChannel, final ByteBuffer inputBuffer) {
		inputBuffer.clear();
		try {
			final int bytesRead = fileChannel.read(inputBuffer);
			readCount = readCount + bytesRead / SizeConstants.BLOCK_SIZE;
			if(bytesRead % SizeConstants.BLOCK_SIZE != 0) {
				readCount++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		inputBuffer.flip();
	}
	
	public int getReadCount() {
		return readCount;
	}
	
	public void writeOutputFile(final FileChannel fileChannel, final ByteBuffer outputBuffer) {
		try {
			final int byteWrites = fileChannel.write(outputBuffer) ;
			writeCount = writeCount + byteWrites / SizeConstants.BLOCK_SIZE ;
			if(byteWrites % SizeConstants.BLOCK_SIZE != 0) {
				writeCount++;
			}
		} catch ( IOException e ) {
			e.printStackTrace();
		}
		outputBuffer.clear() ;
	}

	public int getWriteCount() {
		return writeCount;
	}

	public void deleteOldFiles(final String filePath) {
		final File oldFiles = new File(filePath);
		for(final File oldFile : oldFiles.listFiles()) {
			if(!oldFile.isDirectory()) {
				oldFile.delete();
			}
		}
	}
}
