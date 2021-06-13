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

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

import com.roi.galegot.sequal.sequalmodel.exceptions.InvalidSequenceException;

/**
 * The Class Sequence.
 */
@SuppressWarnings("serial")
public class Sequence implements Serializable {

	private String name;
	private String sequenceString;
	private String extra;
	private String qualityString;

	private int length;
	private int guaCyt;
	private int nAmb;

	private double guaCytP;
	private double nAmbP;
	private double quality;

	private Boolean hasQuality;

	// Parameters for paired sequence

	private Boolean isPaired;
	private String namePair;
	private String sequenceStringPair;
	private String extraPair;
	private String qualityStringPair;

	private int lengthPair;
	private int guaCytPair;
	private int nAmbPair;

	private double guaCytPPair;
	private double nAmbPPair;
	private double qualityPair;

	/**
	 * Instantiates a new sequence.
	 *
	 * @param line1 the line 1
	 * @param line2 the line 2
	 * @throws InvalidSequenceException the invalid sequence exception
	 */
	public Sequence(String line1, String line2) {
		if (!SequenceUtils.checkFastaIsWellFormed(line1, line2)) {
			throw new InvalidSequenceException();
		}

		this.name = line1;
		this.sequenceString = line2;
		this.length = this.sequenceString.length();
		this.guaCyt = SequenceUtils.calculateGuanineCitosyne(this.sequenceString);
		this.guaCytP = SequenceUtils.calculatePercentage(this.guaCyt, this.sequenceString);
		this.nAmb = SequenceUtils.calculateNAmbiguous(this.sequenceString);
		this.nAmbP = SequenceUtils.calculatePercentage(this.nAmb, this.sequenceString);
		this.hasQuality = false;
		this.isPaired = false;
	}

	/**
	 * Instantiates a new sequence.
	 *
	 * @param line1 the line 1
	 * @param line2 the line 2
	 * @param line3 the line 3
	 * @param line4 the line 4
	 * @throws InvalidSequenceException the invalid sequence exception
	 */
	public Sequence(String line1, String line2, String line3, String line4) {
		if (!SequenceUtils.checkFastQIsWellFormed(line1, line2, line3, line4)) {
			throw new InvalidSequenceException();
		}

		this.name = line1;
		this.sequenceString = line2;
		this.extra = line3;
		this.qualityString = line4;
		this.length = this.sequenceString.length();
		this.quality = SequenceUtils.calculateQuality(this.qualityString);
		this.guaCyt = SequenceUtils.calculateGuanineCitosyne(this.sequenceString);
		this.guaCytP = SequenceUtils.calculatePercentage(this.guaCyt, this.sequenceString);
		this.nAmb = SequenceUtils.calculateNAmbiguous(this.sequenceString);
		this.nAmbP = SequenceUtils.calculatePercentage(this.nAmb, this.sequenceString);
		this.hasQuality = true;
		this.isPaired = false;
	}

	public void setPairSequence(String line1, String line2) {
		if (this.hasQuality || !SequenceUtils.checkFastaIsWellFormed(line1, line2)
				|| !SequenceUtils.checkIsAValidFastaPair(this.sequenceString, line2)) {
			throw new InvalidSequenceException();
		}

		this.namePair = line1;
		this.sequenceStringPair = line2;
		this.lengthPair = this.sequenceStringPair.length();
		this.guaCytPair = SequenceUtils.calculateGuanineCitosyne(this.sequenceStringPair);
		this.guaCytPPair = SequenceUtils.calculatePercentage(this.guaCytPair, this.sequenceStringPair);
		this.nAmbPair = SequenceUtils.calculateNAmbiguous(this.sequenceStringPair);
		this.nAmbPPair = SequenceUtils.calculatePercentage(this.nAmbPair, this.sequenceStringPair);
		this.hasQuality = false;
		this.isPaired = true;
	}

	public void setPairSequence(String line1, String line2, String line3, String line4) {
		if (!this.hasQuality || !SequenceUtils.checkFastQIsWellFormed(line1, line2, line3, line4)
				|| !SequenceUtils.checkIsAValidFastQPair(this.sequenceString, this.qualityString, line2, line4)) {
			throw new InvalidSequenceException();
		}

		this.namePair = line1;
		this.sequenceStringPair = line2;
		this.extraPair = line3;
		this.qualityStringPair = line4;
		this.lengthPair = this.sequenceStringPair.length();
		this.qualityPair = SequenceUtils.calculateQuality(this.qualityStringPair);
		this.guaCytPair = SequenceUtils.calculateGuanineCitosyne(this.sequenceStringPair);
		this.guaCytPPair = SequenceUtils.calculatePercentage(this.guaCytPair, this.sequenceStringPair);
		this.nAmbPair = SequenceUtils.calculateNAmbiguous(this.sequenceStringPair);
		this.nAmbPPair = SequenceUtils.calculatePercentage(this.nAmbPair, this.sequenceStringPair);
		this.hasQuality = true;
		this.isPaired = true;
	}

	/**
	 * Gets the name.
	 *
	 * @return the name
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * Sets the name.
	 *
	 * @param name the new name
	 */
	public void setName(String name) {
		if (!(SequenceUtils.checkStart(name, "@") || SequenceUtils.checkStart(name, ">"))) {
			throw new InvalidSequenceException();
		}
		this.name = name;
	}

	/**
	 * Gets the sequence string.
	 *
	 * @return the sequence string
	 */
	public String getSequenceString() {
		return this.sequenceString;
	}

	/**
	 * Sets the sequence string.
	 *
	 * @param sequenceString the new sequence string
	 */
	public void setSequenceString(String sequenceString) {
		this.sequenceString = sequenceString;
		this.length = sequenceString.length();
		this.guaCyt = SequenceUtils.calculateGuanineCitosyne(sequenceString);
		this.guaCytP = SequenceUtils.calculatePercentage(this.guaCyt, this.sequenceString);
		this.nAmb = SequenceUtils.calculateNAmbiguous(sequenceString);
		this.nAmbP = SequenceUtils.calculatePercentage(this.nAmb, this.sequenceString);
	}

	/**
	 * Gets the extra.
	 *
	 * @return the extra
	 */
	public String getExtra() {
		return this.extra;
	}

	/**
	 * Sets the extra.
	 *
	 * @param extra the new extra
	 */
	public void setExtra(String extra) {
		this.extra = extra;
	}

	/**
	 * Gets the quality string.
	 *
	 * @return the quality string
	 */
	public String getQualityString() {
		return this.qualityString;
	}

	/**
	 * Sets the quality string.
	 *
	 * @param qualityString the new quality string
	 */
	public void setQualityString(String qualityString) {
		if (StringUtils.isBlank(qualityString)) {
			this.hasQuality = false;
			this.qualityString = null;
		} else {
			this.qualityString = qualityString;
			this.quality = SequenceUtils.calculateQuality(qualityString);
		}
	}

	/**
	 * Gets the length.
	 *
	 * @return the length
	 */
	public int getLength() {
		return this.length;
	}

	/**
	 * Gets the gua cyt.
	 *
	 * @return the gua cyt
	 */
	public int getGuaCyt() {
		return this.guaCyt;
	}

	/**
	 * Gets the n amb.
	 *
	 * @return the n amb
	 */
	public int getnAmb() {
		return this.nAmb;
	}

	/**
	 * Gets the gua cyt P.
	 *
	 * @return the gua cyt P
	 */
	public double getGuaCytP() {
		return this.guaCytP;
	}

	/**
	 * Gets the n amb P.
	 *
	 * @return the n amb P
	 */
	public double getnAmbP() {
		return this.nAmbP;
	}

	/**
	 * Gets the quality.
	 *
	 * @return the quality
	 */
	public double getQuality() {
		return this.quality;
	}

	/**
	 * Gets the checks for quality.
	 *
	 * @return the checks for quality
	 */
	public Boolean getHasQuality() {
		return this.hasQuality;
	}

	/**
	 * Gets the checks if is paired.
	 *
	 * @return the checks if is paired
	 */
	public Boolean getIsPaired() {
		return this.isPaired;
	}

	/**
	 * Gets the name pair.
	 *
	 * @return the name pair
	 */
	public String getNamePair() {
		return this.namePair;
	}

	// Getters and setters for paired sequence

	/**
	 * Sets the name pair.
	 *
	 * @param namePair the new name pair
	 */
	public void setNamePair(String namePair) {
		if (!(SequenceUtils.checkStart(namePair, "@") || SequenceUtils.checkStart(namePair, ">"))) {
			throw new InvalidSequenceException();
		}
		this.namePair = namePair;
	}

	/**
	 * Gets the sequence string pair.
	 *
	 * @return the sequence string pair
	 */
	public String getSequenceStringPair() {
		return this.sequenceStringPair;
	}

	/**
	 * Sets the sequence string pair.
	 *
	 * @param sequenceStringPair the new sequence string pair
	 */
	public void setSequenceStringPair(String sequenceStringPair) {
		this.sequenceStringPair = sequenceStringPair;
		this.lengthPair = sequenceStringPair.length();
		this.guaCytPair = SequenceUtils.calculateGuanineCitosyne(sequenceStringPair);
		this.guaCytPPair = SequenceUtils.calculatePercentage(this.guaCytPair, this.sequenceStringPair);
		this.nAmbPair = SequenceUtils.calculateNAmbiguous(sequenceStringPair);
		this.nAmbPPair = SequenceUtils.calculatePercentage(this.nAmbPair, this.sequenceStringPair);
	}

	/**
	 * Gets the extra pair.
	 *
	 * @return the extra pair
	 */
	public String getExtraPair() {
		return this.extraPair;
	}

	/**
	 * Sets the extra pair.
	 *
	 * @param extraPair the new extra pair
	 */
	public void setExtraPair(String extraPair) {
		this.extraPair = extraPair;
	}

	/**
	 * Gets the quality string pair.
	 *
	 * @return the quality string pair
	 */
	public String getQualityStringPair() {
		return this.qualityStringPair;
	}

	/**
	 * Sets the quality string pair.
	 *
	 * @param qualityStringPair the new quality string pair
	 */
	public void setQualityStringPair(String qualityStringPair) {
		if (StringUtils.isBlank(qualityStringPair)) {
			this.hasQuality = false;
			this.qualityStringPair = null;
		} else {
			this.qualityStringPair = qualityStringPair;
			this.qualityPair = SequenceUtils.calculateQuality(qualityStringPair);
		}
	}

	/**
	 * Gets the length pair.
	 *
	 * @return the length pair
	 */
	public int getLengthPair() {
		return this.lengthPair;
	}

	/**
	 * Gets the gua cyt pair.
	 *
	 * @return the gua cyt pair
	 */
	public int getGuaCytPair() {
		return this.guaCytPair;
	}

	/**
	 * Gets the n amb pair.
	 *
	 * @return the n amb pair
	 */
	public int getnAmbPair() {
		return this.nAmbPair;
	}

	/**
	 * Gets the gua cyt P pair.
	 *
	 * @return the gua cyt P pair
	 */
	public double getGuaCytPPair() {
		return this.guaCytPPair;
	}

	/**
	 * Gets the n amb P pair.
	 *
	 * @return the n amb P pair
	 */
	public double getnAmbPPair() {
		return this.nAmbPPair;
	}

	/**
	 * Gets the quality pair.
	 *
	 * @return the quality pair
	 */
	public double getQualityPair() {
		return this.qualityPair;
	}

	@Override
	public String toString() {
		if (this.hasQuality) {
			if (this.isPaired) {
				return this.name + "\n" + this.sequenceString + "\n" + this.extra + "\n" + this.qualityString + "\n"
						+ this.namePair + "\n" + this.sequenceStringPair + "\n" + this.extraPair + "\n"
						+ this.qualityStringPair;
			} else {
				return this.name + "\n" + this.sequenceString + "\n" + this.extra + "\n" + this.qualityString;
			}
		} else {
			if (this.isPaired) {
				return this.name + "\n" + this.sequenceString + "\n" + this.namePair + "\n" + this.sequenceStringPair;
			} else {
				return this.name + "\n" + this.sequenceString;
			}
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + ((this.qualityString == null) ? 0 : this.qualityString.hashCode());
		result = (prime * result) + ((this.sequenceString == null) ? 0 : this.sequenceString.hashCode());
		if (this.isPaired) {
			result = (prime * result) + ((this.qualityStringPair == null) ? 0 : this.qualityStringPair.hashCode());
			result = (prime * result) + ((this.sequenceStringPair == null) ? 0 : this.sequenceStringPair.hashCode());
		}
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (this.getClass() != obj.getClass()) {
			return false;
		}
		Sequence other = (Sequence) obj;
		if (this.qualityString == null) {
			if (other.qualityString != null) {
				return false;
			}
		} else if (!this.qualityString.equals(other.qualityString)) {
			return false;
		}
		if (this.sequenceString == null) {
			if (other.sequenceString != null) {
				return false;
			}
		} else if (!this.sequenceString.equals(other.sequenceString)) {
			return false;
		}
		if (this.isPaired) {
			if (!other.isPaired) {
				return false;
			}
			if (this.sequenceStringPair == null) {
				if (other.sequenceStringPair != null) {
					return false;
				}
			} else if (!this.sequenceStringPair.equals(other.sequenceStringPair)) {
				return false;
			}
			if (this.qualityStringPair == null) {
				if (other.qualityStringPair != null) {
					return false;
				}
			} else if (!this.qualityStringPair.equals(other.qualityStringPair)) {
				return false;
			}
		} else if (other.isPaired) {
			return false;
		}
		return true;
	}

}