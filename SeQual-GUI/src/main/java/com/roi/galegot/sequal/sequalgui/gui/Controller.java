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
package com.roi.galegot.sequal.sequalgui.gui;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.roi.galegot.sequal.sequalgui.util.SoutToQueuePrinter;
import com.roi.galegot.sequal.sequalmodel.filter.FilterParametersNaming;
import com.roi.galegot.sequal.sequalmodel.filter.Filters;
import com.roi.galegot.sequal.sequalmodel.formatter.FormatterParametersNaming;
import com.roi.galegot.sequal.sequalmodel.formatter.Formatters;
import com.roi.galegot.sequal.sequalmodel.service.AppService;
import com.roi.galegot.sequal.sequalmodel.stat.StatParametersNaming;
import com.roi.galegot.sequal.sequalmodel.stat.StatsNaming;
import com.roi.galegot.sequal.sequalmodel.trimmer.TrimmerParametersNaming;
import com.roi.galegot.sequal.sequalmodel.trimmer.Trimmers;

import javafx.animation.AnimationTimer;
import javafx.application.Platform;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ComboBox;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.layout.AnchorPane;
import javafx.stage.DirectoryChooser;
import javafx.stage.FileChooser;
import javafx.stage.FileChooser.ExtensionFilter;

public class Controller {

	// Input options
	@FXML
	private TextField inputFileField;
	@FXML
	private CheckBox doubleInputCheckBox;
	@FXML
	private TextField secondInputFileField;
	@FXML
	private TextField outputDirectoryField;
	@FXML
	private TextField outputNameField;
	@FXML
	private CheckBox singleOutputCheckBox;
	@FXML
	private ComboBox<String> logLevelCombo;
	@FXML
	private TextField sparkMasterConfField;
	@FXML
	private CheckBox filterCheckBox;
	@FXML
	private CheckBox trimmerCheckBox;
	@FXML
	private CheckBox formatterCheckBox;
	@FXML
	private CheckBox statisticsCheckBox;
	@FXML
	private ProgressIndicator progressIndicator;
	@FXML
	private AnchorPane runJobPane;

	// Output text area
	@FXML
	private TextArea outputTextArea;

	// Fields for filters
	@FXML
	private CheckBox filterLengthCheckBox;
	@FXML
	private TextField filterLengthMinValueTextField;
	@FXML
	private TextField filterLengthMaxValueTextField;
	@FXML
	private CheckBox filterMeanQualityCheckBox;
	@FXML
	private TextField filterMeanQualityMinValueTextField;
	@FXML
	private TextField filterMeanQualityMaxValueTextField;
	@FXML
	private CheckBox filterQualityScoreCheckBox;
	@FXML
	private TextField filterQualityScoreMinValueTextField;
	@FXML
	private TextField filterQualityScoreMaxValueTextField;
	@FXML
	private CheckBox filterGCBasesCheckBox;
	@FXML
	private TextField filterGCBasesMinValueTextField;
	@FXML
	private TextField filterGCBasesMaxValueTextField;
	@FXML
	private CheckBox filterGCContentCheckBox;
	@FXML
	private TextField filterGCContentMinValueTextField;
	@FXML
	private TextField filterGCContentMaxValueTextField;
	@FXML
	private CheckBox filterNAmbCheckBox;
	@FXML
	private TextField filterNAmbMinValueTextField;
	@FXML
	private TextField filterNAmbMaxValueTextField;
	@FXML
	private CheckBox filterNAmbPCheckBox;
	@FXML
	private TextField filterNAmbPMinValueTextField;
	@FXML
	private TextField filterNAmbPMaxValueTextField;
	@FXML
	private CheckBox filterBaseNCheckBox;
	@FXML
	private TextField filterBaseNBasesTextField;
	@FXML
	private TextField filterBaseNMinValueTextField;
	@FXML
	private TextField filterBaseNMaxValueTextField;
	@FXML
	private CheckBox filterBasePCheckBox;
	@FXML
	private TextField filterBasePBasesTextField;
	@FXML
	private TextField filterBasePMinValueTextField;
	@FXML
	private TextField filterBasePMaxValueTextField;
	@FXML
	private CheckBox filterPatternCheckBox;
	@FXML
	private TextField filterPatternValueTextField;
	@FXML
	private TextField filterPatternRepsTextField;
	@FXML
	private CheckBox filterNoPatternCheckBox;
	@FXML
	private TextField filterNoPatternValueTextField;
	@FXML
	private TextField filterNoPatternRepsTextField;
	@FXML
	private CheckBox filterNonIupacCheckBox;
	@FXML
	private CheckBox filterDistinctCheckBox;
	@FXML
	private CheckBox filterAlmostDistinctCheckBox;
	@FXML
	private TextField filterAlmostDistinctDiffValueTextField;
	@FXML
	private CheckBox filterComplementaryDistinctCheckBox;
	@FXML
	private CheckBox filterReverseDistinctCheckBox;
	@FXML
	private CheckBox filterReverseComplementaryDistinctCheckBox;

	// Fields for trimmers
	@FXML
	private CheckBox trimLeftCheckBox;
	@FXML
	private TextField trimLeftTextField;
	@FXML
	private CheckBox trimRightCheckBox;
	@FXML
	private TextField trimRightTextField;
	@FXML
	private CheckBox trimLeftPCheckBox;
	@FXML
	private TextField trimLeftPTextField;
	@FXML
	private CheckBox trimRightPCheckBox;
	@FXML
	private TextField trimRightPTextField;
	@FXML
	private CheckBox trimLeftToLengthCheckBox;
	@FXML
	private TextField trimLeftToLengthTextField;
	@FXML
	private CheckBox trimRightToLengthCheckBox;
	@FXML
	private TextField trimRightToLengthTextField;
	@FXML
	private CheckBox trimQualityLeftCheckBox;
	@FXML
	private TextField trimQualityLeftTextField;
	@FXML
	private CheckBox trimQualityRightCheckBox;
	@FXML
	private TextField trimQualityRightTextField;
	@FXML
	private CheckBox trimNLeftCheckBox;
	@FXML
	private TextField trimNLeftTextField;
	@FXML
	private CheckBox trimNRightCheckBox;
	@FXML
	private TextField trimNRightTextField;

	// Fields for formatters
	@FXML
	private CheckBox dnaToRnaCheckBox;
	@FXML
	private CheckBox rnaToDnaCheckBox;
	@FXML
	private CheckBox fastqToFastaCheckBox;

	// Fields for statistics
	@FXML
	private CheckBox countCheckBox;
	@FXML
	private CheckBox meanLengthCheckBox;
	@FXML
	private CheckBox meanQualityCheckBox;

	private static final Logger LOGGER = Logger.getLogger(Controller.class.getName());

	@FXML
	private void initialize() {
		this.initializeLogLevelCombo();
		this.setOutToTextArea();

	}

	private void initializeLogLevelCombo() {
//		"TRACE", "DEBUG" Ignored because they block the UI with its verbosity.
		this.logLevelCombo.getItems().addAll("INFO", "WARN", "ERROR", "FATAL");
		this.logLevelCombo.setValue("ERROR");
		this.secondInputFileField.disableProperty().bind(this.doubleInputCheckBox.selectedProperty());
	}

	private void setOutToTextArea() {
		BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();
		SoutToQueuePrinter queuePrinter = new SoutToQueuePrinter(messageQueue);
		PrintStream printStream = new PrintStream(queuePrinter, true);

		System.setOut(printStream);
		System.setErr(printStream);

		Platform.runLater(() -> new MessageConsumer(messageQueue, this.outputTextArea).start());

		ConsoleAppender console = new ConsoleAppender();
		console.setName("console.appender");
		console.setLayout(new PatternLayout("%m%n"));
		console.setThreshold(Level.ALL);
		console.activateOptions();
		Logger.getRootLogger().addAppender(console);

		LogManager.getCurrentLoggers();

		LOGGER.info("Welcome to SeQual!\n"
				+ "To run a job, select you options and fill all mandatory fields marked by *, and then press the button.");
	}

	public class MessageConsumer extends AnimationTimer {
		private final BlockingQueue<String> messageQueue;
		private final TextArea textArea;

		public MessageConsumer(BlockingQueue<String> messageQueue, TextArea textArea) {
			this.messageQueue = messageQueue;
			this.textArea = textArea;
		}

		@Override
		public void handle(long now) {
			List<String> messages = new ArrayList<>();
			this.messageQueue.drainTo(messages);
			messages.forEach(msg -> this.textArea.appendText(msg));
		}
	}

	@FXML
	private void chooseInputFile() {
		this.inputFileField.setText(this.fileChooser());
	}

	@FXML
	private void chooseSecondInputFile() {
		this.secondInputFileField.setText(this.fileChooser());
	}

	private String fileChooser() {
		FileChooser fileChooser = new FileChooser();
		fileChooser.setTitle("Select a FASTQ or FASTA file");
		fileChooser.getExtensionFilters().addAll(new ExtensionFilter("Valid Files", "*.fastq", "*.fq", "*.FASTQ",
				"*.FQ", "*.fasta", "*.fa", "*.FASTA", "*.FA"));

		File selectedFile = fileChooser.showOpenDialog(null);

		if (selectedFile != null) {
			return selectedFile.getPath();
		}

		return "";
	}

	@FXML
	private void chooseOutputDirectory() {
		DirectoryChooser directoryChooser = new DirectoryChooser();
		File selectedDirectory = directoryChooser.showDialog(null);

		if (selectedDirectory != null) {
			this.outputDirectoryField.setText(selectedDirectory.getPath());
		}
	}

	private void runJob() {
		AppService appService = new AppService();

		String input = this.inputFileField.getText();
		String secondInput = this.secondInputFileField.getText();
		String outputDirectory = this.outputDirectoryField.getText();
		String outputName = this.outputNameField.getText();
		String masterConf = this.sparkMasterConfField.getText();
		String logLevelString = this.logLevelCombo.getValue();

		appService.setInput(input);

		if (this.doubleInputCheckBox.isSelected() && StringUtils.isNotBlank(secondInput)) {
			appService.setSecondInput(secondInput);
		}

		appService.setOutput(outputDirectory + "/" + outputName);

		if (StringUtils.isNotBlank(logLevelString)) {
			Level logLevel = Level.toLevel(logLevelString, Level.ERROR);
			appService.setLogLevel(logLevel);
		}

		if (StringUtils.isNotBlank(masterConf)) {
			appService.setMasterConf(masterConf);
		}

		appService.start();

		try {
			appService.read();

			Boolean statisticsBoxIsSelected = this.statisticsCheckBox.isSelected();

			if (statisticsBoxIsSelected) {
				this.parseStatistics(appService);
				appService.measure(true);
			}

			if (this.trimmerCheckBox.isSelected()) {
				this.parseTrimmers(appService);
				appService.trim();
			}

			if (this.filterCheckBox.isSelected()) {
				this.parseFilters(appService);
				appService.filter();
			}

			if (this.formatterCheckBox.isSelected()) {
				this.parseFormatters(appService);
				appService.format();
			}

			if (statisticsBoxIsSelected) {
				appService.measure(false);
			}

			if (this.singleOutputCheckBox.isSelected()) {
				appService.writeWithSingleFile();
			} else {
				appService.write();
			}

			if (statisticsBoxIsSelected) {
				appService.printStats();
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			appService.stop();
		}
	}

	@FXML
	private void startJob() {
		this.progressIndicator.setVisible(true);
		this.runJobPane.setDisable(true);

		Task<Void> jobTask = new Task<Void>() {
			@Override
			protected Void call() throws Exception {
				Controller.this.runJob();
				return null;
			}
		};

		jobTask.setOnSucceeded(t -> {
			Controller.this.progressIndicator.setVisible(false);
			Controller.this.runJobPane.setDisable(false);
		});
		jobTask.setOnFailed(t -> {
			Controller.this.progressIndicator.setVisible(false);
			Controller.this.runJobPane.setDisable(false);
			System.out.println(t.getTarget());
		});

		new Thread(jobTask).start();

	}

	private void parseFilters(AppService appService) {
		String singleFiltersList = "";
		String doubleFiltersList = "";

		// Single filters
		// Length
		if (this.filterLengthCheckBox.isSelected()) {
			appService.setParameter(FilterParametersNaming.LENGTH_MIN_VAL,
					this.filterLengthMinValueTextField.getText());
			appService.setParameter(FilterParametersNaming.LENGTH_MAX_VAL,
					this.filterLengthMaxValueTextField.getText());
			doubleFiltersList = doubleFiltersList.concat("|" + Filters.LENGTH.name());
		}
		// Mean Quality
		if (this.filterMeanQualityCheckBox.isSelected()) {
			appService.setParameter(FilterParametersNaming.QUALITY_MIN_VAL,
					this.filterMeanQualityMinValueTextField.getText());
			appService.setParameter(FilterParametersNaming.QUALITY_MAX_VAL,
					this.filterMeanQualityMaxValueTextField.getText());
			doubleFiltersList = doubleFiltersList.concat("|" + Filters.QUALITY.name());
		}
		// Quality Score
		if (this.filterQualityScoreCheckBox.isSelected()) {
			appService.setParameter(FilterParametersNaming.QUALITY_SCORE_MIN_VAL,
					this.filterQualityScoreMinValueTextField.getText());
			appService.setParameter(FilterParametersNaming.QUALITY_SCORE_MAX_VAL,
					this.filterQualityScoreMaxValueTextField.getText());
			doubleFiltersList = doubleFiltersList.concat("|" + Filters.QUALITYSCORE.name());
		}
		// GCBases
		if (this.filterGCBasesCheckBox.isSelected()) {
			appService.setParameter(FilterParametersNaming.GCBASES_MIN_VAL,
					this.filterGCBasesMinValueTextField.getText());
			appService.setParameter(FilterParametersNaming.GCBASES_MAX_VAL,
					this.filterGCBasesMaxValueTextField.getText());
			doubleFiltersList = doubleFiltersList.concat("|" + Filters.GCBASES.name());
		}
		// GCContent
		if (this.filterGCContentCheckBox.isSelected()) {
			appService.setParameter(FilterParametersNaming.GCCONTENT_MIN_VAL,
					this.filterGCContentMinValueTextField.getText());
			appService.setParameter(FilterParametersNaming.GCCONTENT_MAX_VAL,
					this.filterGCContentMaxValueTextField.getText());
			doubleFiltersList = doubleFiltersList.concat("|" + Filters.GCCONTENT.name());
		}
		// NAmb
		if (this.filterNAmbCheckBox.isSelected()) {
			appService.setParameter(FilterParametersNaming.NAMB_MIN_VAL, this.filterNAmbMinValueTextField.getText());
			appService.setParameter(FilterParametersNaming.NAMB_MAX_VAL, this.filterNAmbMaxValueTextField.getText());
			doubleFiltersList = doubleFiltersList.concat("|" + Filters.NAMB.name());
		}
		// NAmbP
		if (this.filterNAmbPCheckBox.isSelected()) {
			appService.setParameter(FilterParametersNaming.NAMBP_MIN_VAL, this.filterNAmbPMinValueTextField.getText());
			appService.setParameter(FilterParametersNaming.NAMBP_MAX_VAL, this.filterNAmbPMaxValueTextField.getText());
			doubleFiltersList = doubleFiltersList.concat("|" + Filters.NAMBP.name());
		}
		// Base
		if (this.filterBaseNCheckBox.isSelected()) {
			appService.setParameter(FilterParametersNaming.BASE, this.filterBaseNBasesTextField.getText());
			appService.setParameter(FilterParametersNaming.BASE_MIN_VAL, this.filterBaseNMinValueTextField.getText());
			appService.setParameter(FilterParametersNaming.BASE_MAX_VAL, this.filterBaseNMaxValueTextField.getText());
			doubleFiltersList = doubleFiltersList.concat("|" + Filters.BASEN.name());
		}
		// Base P
		if (this.filterBasePCheckBox.isSelected()) {
			appService.setParameter(FilterParametersNaming.BASEP, this.filterBasePBasesTextField.getText());
			appService.setParameter(FilterParametersNaming.BASEP_MIN_VAL, this.filterBasePMinValueTextField.getText());
			appService.setParameter(FilterParametersNaming.BASEP_MAX_VAL, this.filterBasePMaxValueTextField.getText());
			doubleFiltersList = doubleFiltersList.concat("|" + Filters.BASEP.name());
		}
		// Pattern
		if (this.filterPatternCheckBox.isSelected()) {
			appService.setParameter(FilterParametersNaming.PATTERN, this.filterPatternValueTextField.getText());
			appService.setParameter(FilterParametersNaming.REP_PATTERN, this.filterPatternRepsTextField.getText());
			doubleFiltersList = doubleFiltersList.concat("|" + Filters.PATTERN.name());
		}
		// No-Pattern
		if (this.filterNoPatternCheckBox.isSelected()) {
			appService.setParameter(FilterParametersNaming.NO_PATTERN, this.filterNoPatternValueTextField.getText());
			appService.setParameter(FilterParametersNaming.REP_NO_PATTERN, this.filterNoPatternRepsTextField.getText());
			doubleFiltersList = doubleFiltersList.concat("|" + Filters.NOPATTERN.name());
		}
		// Non-IUPAC
		if (this.filterNonIupacCheckBox.isSelected()) {
			doubleFiltersList = doubleFiltersList.concat("|" + Filters.NONIUPAC.name());
		}

		// Group filters
		// Distinct
		if (this.filterDistinctCheckBox.isSelected()) {
			doubleFiltersList = doubleFiltersList.concat("|" + Filters.DISTINCT.name());
		}
		// Almost distinct
		if (this.filterAlmostDistinctCheckBox.isSelected()) {
			appService.setParameter(FilterParametersNaming.MAX_DIFFERENCE,
					this.filterAlmostDistinctDiffValueTextField.getText());
			doubleFiltersList = doubleFiltersList.concat("|" + Filters.ALMOSTDISTINCT.name());
		}
		// Reverse distinct
		if (this.filterReverseDistinctCheckBox.isSelected()) {
			doubleFiltersList = doubleFiltersList.concat("|" + Filters.REVERSEDISTINCT.name());
		}
		// Complement distinct
		if (this.filterComplementaryDistinctCheckBox.isSelected()) {
			doubleFiltersList = doubleFiltersList.concat("|" + Filters.COMPLEMENTDISTINCT.name());
		}
		// Reverse complement distinct
		if (this.filterReverseComplementaryDistinctCheckBox.isSelected()) {
			doubleFiltersList = doubleFiltersList.concat("|" + Filters.REVERSECOMPLEMENTDISTINCT.name());
		}

		appService.setParameter(FilterParametersNaming.SINGLE_FILTERS_LIST, singleFiltersList);
		appService.setParameter(FilterParametersNaming.GROUP_FILTERS_LIST, doubleFiltersList);
	}

	private void parseTrimmers(AppService appService) {
		String trimmersList = "";

		// Trim left
		if (this.trimLeftCheckBox.isSelected()) {
			appService.setParameter(TrimmerParametersNaming.TRIM_LEFT, this.trimLeftTextField.getText());
			trimmersList = trimmersList.concat("|" + Trimmers.TRIMLEFT.name());
		}
		// Trim right
		if (this.trimRightCheckBox.isSelected()) {
			appService.setParameter(TrimmerParametersNaming.TRIM_RIGHT, this.trimRightTextField.getText());
			trimmersList = trimmersList.concat("|" + Trimmers.TRIMRIGHT.name());
		}
		// Trim left p
		if (this.trimLeftPCheckBox.isSelected()) {
			appService.setParameter(TrimmerParametersNaming.TRIM_LEFTP, this.trimLeftPTextField.getText());
			trimmersList = trimmersList.concat("|" + Trimmers.TRIMLEFTP.name());
		}
		// Trim right
		if (this.trimRightPCheckBox.isSelected()) {
			appService.setParameter(TrimmerParametersNaming.TRIM_RIGHTP, this.trimRightPTextField.getText());
			trimmersList = trimmersList.concat("|" + Trimmers.TRIMRIGHTP.name());
		}
		// Trim left to length
		if (this.trimLeftToLengthCheckBox.isSelected()) {
			appService.setParameter(TrimmerParametersNaming.TRIM_LEFT_TO_LENGTH,
					this.trimLeftToLengthTextField.getText());
			trimmersList = trimmersList.concat("|" + Trimmers.TRIMLEFTTOLENGTH.name());
		}
		// Trim right to length
		if (this.trimRightToLengthCheckBox.isSelected()) {
			appService.setParameter(TrimmerParametersNaming.TRIM_RIGHT_TO_LENGTH,
					this.trimRightToLengthTextField.getText());
			trimmersList = trimmersList.concat("|" + Trimmers.TRIMRIGHTTOLENGTH.name());
		}
		// Trim qual left
		if (this.trimQualityLeftCheckBox.isSelected()) {
			appService.setParameter(TrimmerParametersNaming.TRIM_QUAL_LEFT, this.trimQualityLeftTextField.getText());
			trimmersList = trimmersList.concat("|" + Trimmers.TRIMQUALLEFT.name());
		}
		// Trim qual right
		if (this.trimQualityRightCheckBox.isSelected()) {
			appService.setParameter(TrimmerParametersNaming.TRIM_QUAL_RIGHT, this.trimQualityRightTextField.getText());
			trimmersList = trimmersList.concat("|" + Trimmers.TRIMQUALRIGHT.name());
		}
		// Trim N left
		if (this.trimNLeftCheckBox.isSelected()) {
			appService.setParameter(TrimmerParametersNaming.TRIM_N_LEFT, this.trimNLeftTextField.getText());
			trimmersList = trimmersList.concat("|" + Trimmers.TRIMNLEFT.name());
		}
		// Trim N right
		if (this.trimNRightCheckBox.isSelected()) {
			appService.setParameter(TrimmerParametersNaming.TRIM_N_RIGHT, this.trimNRightTextField.getText());
			trimmersList = trimmersList.concat("|" + Trimmers.TRIMNRIGHT.name());
		}

		appService.setParameter(TrimmerParametersNaming.TRIMMERS_LIST, trimmersList);
	}

	private void parseFormatters(AppService appService) {
		String formattersList = "";

		// DNA To RNA
		if (this.dnaToRnaCheckBox.isSelected()) {
			formattersList = formattersList.concat("|" + Formatters.DNATORNA.name());
		}
		// RNA To DNA
		if (this.rnaToDnaCheckBox.isSelected()) {
			formattersList = formattersList.concat("|" + Formatters.RNATODNA.name());
		}
		// FASTQ To FASTA
		if (this.fastqToFastaCheckBox.isSelected()) {
			formattersList = formattersList.concat("|" + Formatters.FASTQTOFASTA.name());
		}

		appService.setParameter(FormatterParametersNaming.FORMATTERS_LIST, formattersList);
	}

	private void parseStatistics(AppService appService) {
		String statsList = "";

		// Count
		if (this.countCheckBox.isSelected()) {
			statsList = statsList.concat("|" + StatsNaming.COUNT);
		}
		// Mean Length
		if (this.meanLengthCheckBox.isSelected()) {
			statsList = statsList.concat("|" + StatsNaming.MEAN_LENGTH);
		}
		// Mean Quality
		if (this.meanQualityCheckBox.isSelected()) {
			statsList = statsList.concat("|" + StatsNaming.MEAN_QUALITY);
		}

		appService.setParameter(StatParametersNaming.STATISTICS_LIST, statsList);
	}
}
