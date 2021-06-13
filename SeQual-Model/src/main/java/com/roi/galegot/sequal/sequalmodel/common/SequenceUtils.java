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
package com.roi.galegot.sequal.sequalmodel.common;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import org.apache.commons.lang3.StringUtils;

/**
 * The Class SequenceUtils.
 */
public class SequenceUtils {

	/**
	 * Instantiates a new sequence utils.
	 */
	private SequenceUtils() {
	}

	/**
	 * Calculate quality.
	 *
	 * @param qualityString the quality string
	 * @return the double
	 */
	public static Double calculateQuality(String qualityString) {
		double qual = 0;
		for (char c : qualityString.toCharArray()) {
			qual = (qual + c) - 33;
		}
		return qual / qualityString.length();
	}

	/**
	 * Calculate N ambiguous.
	 *
	 * @param sequenceString the sequence string
	 * @return the int
	 */
	public static int calculateNAmbiguous(String sequenceString) {
		int nAmb = 0;
		for (char c : sequenceString.toCharArray()) {
			if (c == 'N') {
				nAmb++;
			}
		}
		return nAmb;
	}

	/**
	 * Calculate guanino citosyne.
	 *
	 * @param sequenceString the sequence string
	 * @return the int
	 */
	public static int calculateGuanineCitosyne(String sequenceString) {
		int guaCyt = 0;
		for (char c : sequenceString.toCharArray()) {
			if ((c == 'G') || (c == 'C')) {
				guaCyt++;
			}
		}
		return guaCyt;
	}

	/**
	 * Check fasta is well formed.
	 *
	 * @param line1 the line 1
	 * @param line2 the line 2
	 * @return the boolean
	 */
	public static Boolean checkFastaIsWellFormed(String line1, String line2) {
		if (StringUtils.isBlank(line1) || StringUtils.isBlank(line2)) {
			return false;
		}
		if (!checkStart(line1, ">")) {
			return false;
		}
		return true;
	}

	/**
	 * Check fast Q is well formed.
	 *
	 * @param line1 the line 1
	 * @param line2 the line 2
	 * @param line3 the line 3
	 * @param line4 the line 4
	 * @return the boolean
	 */
	public static Boolean checkFastQIsWellFormed(String line1, String line2, String line3, String line4) {
		if (StringUtils.isBlank(line1) || StringUtils.isBlank(line2) || StringUtils.isBlank(line3)
				|| StringUtils.isBlank(line4)) {
			return false;
		}
		if (line2.length() != line4.length()) {
			return false;
		}
		if (!checkStart(line1, "@")) {
			return false;
		}
		return true;
	}

	/**
	 * Check start.
	 *
	 * @param line    the line
	 * @param starter the starter
	 * @return the boolean
	 */
	public static Boolean checkStart(String line, String starter) {
		return line.startsWith(starter);
	}

	/**
	 * Calculate percentage.
	 *
	 * @param quantity the quantity
	 * @param string   the string
	 * @return the double
	 */
	public static Double calculatePercentage(int quantity, String string) {
		return (double) quantity / string.length();
	}

	/**
	 * Check is A valid fasta pair.
	 *
	 * @param line1     the line 1
	 * @param line2     the line 2
	 * @param line1Pair the line 1 pair
	 * @param line2Pair the line 2 pair
	 * @return the boolean
	 */
	public static Boolean checkIsAValidFastaPair(String line2, String line2Pair) {
		return (line2.length() == line2Pair.length());
	}

	/**
	 * Check is A valid fast Q pair.
	 *
	 * @param line1     the line 1
	 * @param line2     the line 2
	 * @param line4     the line 4
	 * @param line1Pair the line 1 pair
	 * @param line2Pair the line 2 pair
	 * @param line4Pair the line 4 pair
	 * @return the boolean
	 */
	public static Boolean checkIsAValidFastQPair(String line2, String line4, String line2Pair, String line4Pair) {
		return ((line2.length() == line2Pair.length()) && (line4.length() == line4Pair.length()));
	}

	/**
	 * Gets the first sequence from pair.
	 *
	 * @param sequence the sequence
	 * @return the first sequence from pair
	 */
	public static Sequence getFirstSequenceFromPair(Sequence sequence) {
		return new Sequence(sequence.getName(), sequence.getSequenceString(), sequence.getExtra(),
				sequence.getQualityString());
	}

	/**
	 * Gets the second sequence from pair.
	 *
	 * @param sequence the sequence
	 * @return the second sequence from pair
	 */
	public static Sequence getSecondSequenceFromPair(Sequence sequence) {
		return new Sequence(sequence.getNamePair(), sequence.getSequenceStringPair(), sequence.getExtraPair(),
				sequence.getQualityStringPair());
	}

	/**
	 * Select sequence with max quality.
	 *
	 * @param sequence1 the sequence 1
	 * @param sequence2 the sequence 2
	 * @return the sequence
	 */
	public static Sequence selectSequenceWithMaxQuality(Sequence sequence1, Sequence sequence2) {
		if (sequence1.getIsPaired()) {
			return Collections.max(Arrays.asList(sequence1, sequence2),
					Comparator.comparing(s -> Math.max(s.getQuality(), s.getQualityPair())));
		}

		return Collections.max(Arrays.asList(sequence1, sequence2), Comparator.comparing(s -> s.getQuality()));
	}

}
