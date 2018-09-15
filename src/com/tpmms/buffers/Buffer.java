package com.tpmms.buffers;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;

import com.tpmms.utility.SizeConstants;

public class Buffer {
	private final FileChannel fileChannel;
	private ByteBuffer buffer;
	private long currentPosition;
	private long lastPosition;

	public Buffer(ByteBuffer newBuffer, long currentPosition, long lastPosition, final FileChannel fileChannel) {
		this.buffer = newBuffer;
		this.currentPosition = currentPosition;
		this.lastPosition = lastPosition;
		this.fileChannel = fileChannel;
	}

	public boolean readBuffer() {
		if (this.lastPosition < this.currentPosition) {
			return false;
		}
		this.buffer.clear();
		try {
			this.fileChannel.read(this.buffer, this.currentPosition);
		} catch (IOException e) {
			e.printStackTrace();
		}

		if ((this.lastPosition - this.currentPosition + 1) < this.buffer.capacity()) {
			this.buffer.limit((int) (this.lastPosition - this.currentPosition + 1));
			this.buffer.position(0);
		} else {
			this.buffer.flip();
		}

		this.currentPosition = this.currentPosition + this.buffer.limit();
		return true;
	}

	public int compareTo(final Buffer buffer) {
		int position1 = this.buffer.position();
		int position2 = buffer.getBuffer().position();
		for (int k = 0; k != SizeConstants.TUPLE_SIZE; ++k) {
			try {
				if (this.buffer.get(position1 + k) < buffer.getBuffer().get(position2 + k)) {
					return -1;
				} else if (this.buffer.get(position1 + k) > buffer.getBuffer().get(position2 + k)) {
					return 1;
				}
			}catch(Exception e) {
				e.printStackTrace();
			}
		}
		return 0;
	}

	public FileChannel getFileChannel() {
		return this.fileChannel;
	}

	public ByteBuffer getBuffer() {
		return this.buffer;
	}

	public void setBuffer(ByteBuffer buffer) {
		this.buffer = buffer;
	}

	public long getCurrentPosition() {
		return this.currentPosition;
	}

	public void setCurrentPosition(long currentPosition) {
		this.currentPosition = currentPosition;
	}

	public long getLastPosition() {
		return this.lastPosition;
	}

	public void setLastPosition(long lastPosition) {
		this.lastPosition = lastPosition;
	}
}
