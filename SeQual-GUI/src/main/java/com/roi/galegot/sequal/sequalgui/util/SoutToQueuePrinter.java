/*
 * This file is part of SeQual.
 * 
 * SeQual is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SeQual is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SeQual.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.roi.galegot.sequal.sequalgui.util;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.BlockingQueue;

public class SoutToQueuePrinter extends OutputStream {
	private BlockingQueue<String> messageQueue;

	public SoutToQueuePrinter(BlockingQueue<String> messageQueue) {
		this.messageQueue = messageQueue;
	}

	@Override
	public void write(int i) throws IOException {
		try {
			this.messageQueue.put(String.valueOf((char) i));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
