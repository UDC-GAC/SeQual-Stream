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
package com.roi.galegot.sequal.sequalmodel.dnafilereader;

import org.apache.commons.lang3.StringUtils;

/**
 * A factory for creating DNAFileReader objects.
 */
public class DNAFileReaderFactory {

	/**
	 * Instantiates a new DNA file reader factory.
	 */
	private DNAFileReaderFactory() {
	}

	/**
	 * Creates a DNAFileReader based on the extension of the file to be read.
	 *
	 * @param extension the extension
	 * @return DNAFileReader specifically for that kind of file
	 */
	public static synchronized DNAFileReader getReader(String extension) {
		if (StringUtils.isBlank(extension)) {
			throw new RuntimeException("Supported file formats: FASTQ (.fq .fastq) FASTA(.fa .fasta)");
		}
		switch (extension.toLowerCase()) {
		case "fq":
		case "fastq":
			return new FQReader();

		case "fa":
		case "fasta":
			return new FASTAReader();

		default:
			throw new RuntimeException("Supported file formats: FASTQ (.fq .fastq) FASTA(.fa .fasta)");

		}
	}

	/**
	 * Gets the paired reader.
	 *
	 * @param extension the extension
	 * @return the paired reader
	 */
	public static synchronized DNAPairedFileReader getPairedReader(String extension) {
		if (StringUtils.isBlank(extension)) {
			throw new RuntimeException("Supported file formats: FASTQ (.fq .fastq) FASTA(.fa .fasta)");
		}
		switch (extension.toLowerCase()) {
		case "fq":
		case "fastq":
			return new FQPairedReader();

		case "fa":
		case "fasta":
			return new FASTAPairedReader();

		default:
			throw new RuntimeException("Supported file formats: FASTQ (.fq .fastq) FASTA(.fa .fasta)");

		}
	}
}