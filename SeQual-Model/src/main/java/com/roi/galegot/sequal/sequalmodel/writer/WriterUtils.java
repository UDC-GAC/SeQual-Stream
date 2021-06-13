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
package com.roi.galegot.sequal.sequalmodel.writer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;
import com.roi.galegot.sequal.sequalmodel.common.SequenceUtils;

/**
 * The Class WriterUtils.
 */
public class WriterUtils {

	/** The output folder name first pair. */
	private static final String OUTPUT_FOLDER_NAME_FIRST_PAIR = "/FirstPairs";

	/** The output folder name second pair. */
	private static final String OUTPUT_FOLDER_NAME_SECOND_PAIR = "/SecondPairs";

	/**
	 * Instantiates a new writer utils.
	 */
	public WriterUtils() {
	}

	/**
	 * Write to file.
	 *
	 * @param folderPath     the folder path
	 * @param outputFileName the output file name
	 * @param partsFolder    the parts folder
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void mergeHDFSToFile(String folderPath, String outputFileName, String partsFolder)
			throws IOException {

		File folder = new File(partsFolder);
		File merged = new File(folderPath + "/" + outputFileName);

		if (!merged.exists()) {
			merged.createNewFile();
		}

		PrintWriter printWriter = new PrintWriter(merged);
		String[] fileStringsList = folder.list((dir, name) -> StringUtils.startsWith(name, "part-"));

		for (String fileString : fileStringsList) {
			File file = new File(folder, fileString);
			BufferedReader bufferedReader = new BufferedReader(new FileReader(file));

			String line = "";
			while ((line = bufferedReader.readLine()) != null) {
				printWriter.println(line);
			}

			bufferedReader.close();
		}

		printWriter.flush();
		printWriter.close();
	}

	/**
	 * Write HDFS.
	 *
	 * @param sequences the sequences
	 * @param output    the output
	 */
	public static void writeHDFS(JavaRDD<Sequence> sequences, String output) {
		if (!sequences.isEmpty() && sequences.first().getIsPaired()) {
			writeHDFSPaired(sequences, output);
		} else {
			sequences.saveAsTextFile(output);
		}
	}

	/**
	 * Write HDFS and merge to file.
	 *
	 * @param sequences the sequences
	 * @param output    the output
	 * @param filename  the filename
	 * @param format    the format
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void writeHDFSAndMergeToFile(JavaRDD<Sequence> sequences, String output, String filename,
			String format) throws IOException {

		if (!sequences.isEmpty() && sequences.first().getIsPaired()) {
			String partsFolder1 = output + OUTPUT_FOLDER_NAME_FIRST_PAIR;
			String partsFolder2 = output + OUTPUT_FOLDER_NAME_SECOND_PAIR;

			writeHDFSPaired(sequences, output);

			mergeHDFSToFile(output, filename + "_1-results." + format, partsFolder1);
			mergeHDFSToFile(output, filename + "_2-results." + format, partsFolder2);
		} else {
			String partsFolder = output + "/Parts";

			writeHDFS(sequences, partsFolder);

			mergeHDFSToFile(output, filename + "-results." + format, partsFolder);
		}

	}

	/**
	 * Write HDFS paired.
	 *
	 * @param sequences the sequences
	 * @param output    the output
	 */
	private static void writeHDFSPaired(JavaRDD<Sequence> sequences, String output) {
		JavaRDD<Sequence> firstSequences;
		JavaRDD<Sequence> secondSequences;

		firstSequences = sequences.map(sequence -> SequenceUtils.getFirstSequenceFromPair(sequence));
		secondSequences = sequences.map(sequence -> SequenceUtils.getSecondSequenceFromPair(sequence));

		firstSequences.saveAsTextFile(output + OUTPUT_FOLDER_NAME_FIRST_PAIR);
		secondSequences.saveAsTextFile(output + OUTPUT_FOLDER_NAME_SECOND_PAIR);

	}

}
