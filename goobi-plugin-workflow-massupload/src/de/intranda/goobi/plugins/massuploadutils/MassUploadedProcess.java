package de.intranda.goobi.plugins.massuploadutils;

import java.io.IOException;

import org.goobi.beans.Step;

import de.sub.goobi.helper.exceptions.DAOException;
import de.sub.goobi.helper.exceptions.SwapException;
import lombok.Data;
import lombok.extern.log4j.Log4j;

@Log4j
@Data
public class MassUploadedProcess {
	
	private int stepId;
	private int processId;
	private String processTitle;
	private String processFolder;
	
	public MassUploadedProcess(Step inStep){
		stepId = inStep.getId();
		processId = inStep.getProcessId();
		processTitle = inStep.getProzess().getTitel();
		try {
			processFolder = inStep.getProzess().getImagesOrigDirectory(true);
		} catch (IOException | InterruptedException | SwapException | DAOException e) {
			log.error("Could not get process master folder for process " + processTitle, e);
		}
	}

}
