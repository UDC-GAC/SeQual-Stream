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
package com.oscar.castellanos.sequal.sequalmodel.stream.common;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;

/**
 * The Class SequenceWithTimestamp.
 */
public class SequenceWithTimestamp {
	
	private Sequence sequence;
	private String timestamp;
	
	/**
	 * Instantiates a new sequence.
	 *
	 * @param sequence  the sequence
	 * @param timestamp the timestamp string
	 */
	public SequenceWithTimestamp(Sequence sequence, String timestamp) {
		this.setSequence(sequence);
		this.setTimestamp(timestamp);
	}

	/**
	 * Gets the sequence.
	 *
	 * @return the sequence
	 */
	public Sequence getSequence() {
		return sequence;
	}

	/**
	 * Sets the sequence.
	 *
	 * @param sequence the new sequence
	 */
	public void setSequence(Sequence sequence) {
		this.sequence = sequence;
	}

	/**
	 * Gets the timestamp string.
	 *
	 * @return the timestamp string
	 */
	public String getTimestamp() {
		return timestamp;
	}

	/**
	 * Sets the timestamp string.
	 *
	 * @param timestamp the new timestamp string
	 */
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	
	
}
