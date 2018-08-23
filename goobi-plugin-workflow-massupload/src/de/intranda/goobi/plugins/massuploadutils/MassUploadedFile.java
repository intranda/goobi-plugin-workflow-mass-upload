package de.intranda.goobi.plugins.massuploadutils;

import java.io.File;

import lombok.Data;

@Data
public class MassUploadedFile implements Comparable<MassUploadedFile> {
	
	private File file;
	private String filename;
	private MassUploadedFileStatus status;
	private String statusmessage;
	private int processId;
	private String processTitle;
	private String processFolder;
	private int stepId;
	private int tempId;
	private boolean transfered = false;
	
	public MassUploadedFile(File file, String filename){
		this.file = file;
		this.filename = filename;
		status = MassUploadedFileStatus.UNKNWON;
		statusmessage = "";
	}

	@Override
	public int compareTo(MassUploadedFile o) {
		return this.file.getAbsolutePath().compareTo(o.file.getAbsolutePath());
	}
}
