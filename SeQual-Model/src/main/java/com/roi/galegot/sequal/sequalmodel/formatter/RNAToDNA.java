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
package com.roi.galegot.sequal.sequalmodel.formatter;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;

/**
 * The Class RNAToDNA.
 */
public class RNAToDNA implements Formatter {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 3160351422137802821L;

	@Override
	public JavaRDD<Sequence> format(JavaRDD<Sequence> sequences) {

		if (sequences.first().getIsPaired()) {
			return sequences.map(sequence -> this.doFormatPair(sequence));
		}

		return sequences.map(sequence -> this.doFormat(sequence));
	}

	/**
	 * Do format.
	 *
	 * @param sequence the sequence
	 * @return the sequence
	 */
	private Sequence doFormat(Sequence sequence) {
		sequence.setSequenceString(sequence.getSequenceString().replace("U", "T"));
		return sequence;
	}

	/**
	 * Do format pair.
	 *
	 * @param sequence the sequence
	 * @return the sequence
	 */
	private Sequence doFormatPair(Sequence sequence) {

		this.doFormat(sequence);

		sequence.setSequenceStringPair(sequence.getSequenceStringPair().replace("U", "T"));

		return sequence;
	}
}