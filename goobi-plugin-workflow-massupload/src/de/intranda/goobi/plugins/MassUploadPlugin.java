package de.intranda.goobi.plugins;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.goobi.beans.Process;
import org.goobi.beans.Step;
import org.goobi.beans.User;
import org.goobi.managedbeans.LoginBean;
import org.goobi.production.enums.LogType;
import org.goobi.production.enums.PluginType;
import org.goobi.production.flow.statistics.hibernate.FilterHelper;
import org.goobi.production.plugin.PluginLoader;
import org.goobi.production.plugin.interfaces.IPlugin;
import org.goobi.production.plugin.interfaces.IValidatorPlugin;
import org.goobi.production.plugin.interfaces.IWorkflowPlugin;
import org.primefaces.event.FileUploadEvent;
import org.primefaces.model.UploadedFile;

import de.intranda.goobi.plugins.massuploadutils.GoobiScriptCopyImages;
import de.intranda.goobi.plugins.massuploadutils.MassUploadedFile;
import de.intranda.goobi.plugins.massuploadutils.MassUploadedFileStatus;
import de.intranda.goobi.plugins.massuploadutils.MassUploadedProcess;
import de.sub.goobi.config.ConfigPlugins;
import de.sub.goobi.config.ConfigurationHelper;
import de.sub.goobi.helper.Helper;
import de.sub.goobi.helper.HelperSchritte;
import de.sub.goobi.helper.StorageProvider;
import de.sub.goobi.helper.enums.StepStatus;
import de.sub.goobi.helper.exceptions.DAOException;
import de.sub.goobi.helper.exceptions.SwapException;
import de.sub.goobi.persistence.managers.ProcessManager;
import de.sub.goobi.persistence.managers.StepManager;
import lombok.Data;
import lombok.extern.log4j.Log4j;
import net.xeoh.plugins.base.annotations.PluginImplementation;

@PluginImplementation
@Log4j
@Data
public class MassUploadPlugin implements IWorkflowPlugin, IPlugin {

    private static final String PLUGIN_NAME = "intranda_workflow_massupload";
    private String allowedTypes;
    private String filenamePart;
    private String userFolderName;
    private String filenameSeparator;
    //    private String processnamePart;
    //    private String processnameSeparator;
    private List<String> stepTitles;
    private List<MassUploadedFile> uploadedFiles = new ArrayList<MassUploadedFile>();
    private User user;
    private File tempFolder;
    private HashSet<Integer> stepIDs = new HashSet<>();
    private List<MassUploadedProcess> finishedInserts = new ArrayList<MassUploadedProcess>();
    private boolean copyImagesViaGoobiScript = false;

    /**
     * Constructor
     */
    @SuppressWarnings("unchecked")
    public MassUploadPlugin() {
        log.info("Mass upload plugin started");
        allowedTypes = ConfigPlugins.getPluginConfig(PLUGIN_NAME).getString("allowed-file-extensions", "/(\\.|\\/)(gif|jpe?g|png|tiff?|jp2|pdf)$/");
        filenamePart = ConfigPlugins.getPluginConfig(PLUGIN_NAME).getString("filename-part", "prefix").toLowerCase();
        userFolderName = ConfigPlugins.getPluginConfig(PLUGIN_NAME).getString("user-folder-name", "mass_upload").toLowerCase();
        filenameSeparator = ConfigPlugins.getPluginConfig(PLUGIN_NAME).getString("filename-separator", "_").toLowerCase();
        //    	processnamePart = ConfigPlugins.getPluginConfig(this).getString("processname-part", "complete").toLowerCase();
        //    	processnameSeparator = ConfigPlugins.getPluginConfig(this).getString("processname-separator", "_").toLowerCase();
        stepTitles = ConfigPlugins.getPluginConfig(PLUGIN_NAME).getList("allowed-step");
        copyImagesViaGoobiScript = ConfigPlugins.getPluginConfig(PLUGIN_NAME).getBoolean("copy-images-using-goobiscript", false);
        LoginBean login = (LoginBean) Helper.getManagedBeanValue("#{LoginForm}");
        if (login != null) {
            user = login.getMyBenutzer();
        }
    }

    @Override
    public PluginType getType() {
        return PluginType.Workflow;
    }

    @Override
    public String getTitle() {
        return PLUGIN_NAME;
    }

    @Override
    public String getGui() {
        return "/uii/plugin_workflow_massupload.xhtml";
    }

    /**
     * Handle the upload of a file
     * 
     * @param event
     */
    public void uploadFile(FileUploadEvent event) {
        try {
            if (tempFolder == null) {
                tempFolder = new File(ConfigurationHelper.getInstance().getTemporaryFolder(), user.getLogin());
                if (!tempFolder.exists()) {
                    if (!tempFolder.mkdirs()) {
                        throw new IOException("Upload folder for user could not be created: " + tempFolder.getAbsolutePath());
                    }
                }
            }
            UploadedFile upload = event.getFile();
            saveFileTemporary(upload.getFileName(), upload.getInputstream());
        } catch (IOException e) {
            log.error("Error while uploading files", e);
        }

    }

    public void sortFiles() {
        Collections.sort(uploadedFiles);
    }

    /**
     * Save the uploaded file temporary in the tmp-folder inside of goobi in a subfolder for the user
     * 
     * @param fileName
     * @param in
     * @throws IOException
     */
    private void saveFileTemporary(String fileName, InputStream in) throws IOException {
        if (tempFolder == null) {
            tempFolder = new File(ConfigurationHelper.getInstance().getTemporaryFolder(), user.getLogin());
            if (!tempFolder.exists()) {
                if (!tempFolder.mkdirs()) {
                    throw new IOException("Upload folder for user could not be created: " + tempFolder.getAbsolutePath());
                }
            }
        }

        OutputStream out = null;
        try {
            File file = new File(tempFolder, fileName);
            out = new FileOutputStream(file);
            int read = 0;
            byte[] bytes = new byte[1024];
            while ((read = in.read(bytes)) != -1) {
                out.write(bytes, 0, read);
            }
            out.flush();
            MassUploadedFile muf = new MassUploadedFile(file, fileName);
            assignProcess(muf, null);
            uploadedFiles.add(muf);
        } catch (IOException e) {
            log.error(e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    log.error(e);
                }
            }
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    log.error(e);
                }
            }
        }
    }

    /**
     * do not upload the images from web UI, use images of subfolder in user home directory instead, usually called 'mass_upload'
     */
    public void readFilesFromUserHomeFolder() {
        uploadedFiles = new ArrayList<>();
        finishedInserts = new ArrayList<MassUploadedProcess>();
        stepIDs = new HashSet<>();
        try {
            File folder = new File(user.getHomeDir(), userFolderName);
            if (folder.exists() && folder.canRead()) {
                // we use the Files API intentionally, as we expect folders with many files in them. 
                // The nio DirectoryStream initializes the Path objects lazily, so we don't have as many objects in memory and to create 
                try (DirectoryStream<Path> files = Files.newDirectoryStream(folder.toPath())) {
                    Map<String, List<Process>> searchCache = new HashMap<>();
                    for (Path file : files) {
                        if (!file.getFileName().toString().equals(".DS_Store")) {
                            MassUploadedFile muf = new MassUploadedFile(file.toFile(), file.getFileName().toString());
                            assignProcess(muf, searchCache);
                            uploadedFiles.add(muf);
                        }
                    }
                }
            } else {
                Helper.setFehlerMeldung("Folder " + folder.getAbsolutePath() + " does not exist or is not readable.");
            }
        } catch (Exception e) {
            log.error("Error while reading files from users home directory for mass upload", e);
            Helper.setFehlerMeldung("Error while reading files from users home directory for mass upload", e);
        }

    }

    /**
     * Cancel the entire process and delete the uploaded files
     */
    public void cleanUploadFolder() {
        for (MassUploadedFile uploadedFile : uploadedFiles) {
            uploadedFile.getFile().delete();
        }
        uploadedFiles = new ArrayList<>();
        finishedInserts = new ArrayList<MassUploadedProcess>();
        stepIDs = new HashSet<>();
    }

    /**
     * All uploaded files shall now be moved to the correct processes
     */
    public void startInserting() {

        if (copyImagesViaGoobiScript) {
            GoobiScriptCopyImages gsci = new GoobiScriptCopyImages();
            gsci.setUploadedFiles(uploadedFiles);
            gsci.setUser(user);
            gsci.execute();
            Helper.setMeldung("plugin_massupload_insertionStartedViaGoobiScript");

        } else {
            for (MassUploadedFile muf : uploadedFiles) {
                if (muf.getStatus() == MassUploadedFileStatus.OK) {
                    Path src = Paths.get(muf.getFile().getAbsolutePath());
                    Path target = Paths.get(muf.getProcessFolder(), muf.getFilename());
                    try {
                        StorageProvider.getInstance().copyFile(src, target);
                    } catch (IOException e) {
                        muf.setStatus(MassUploadedFileStatus.ERROR);
                        muf.setStatusmessage("File could not be copied to: " + target.toString());
                        log.error("Error while copying file during mass upload", e);
                        Helper.setFehlerMeldung("Error while copying file during mass upload", e);
                    }
                    muf.getFile().delete();
                } else {
                    Helper.setFehlerMeldung("File could not be matched and gets skipped: " + muf.getFilename());
                }
            }

            // all images are uploaded, so we close the workflow step now
            // first remove all stepIds which had errors
            for (MassUploadedFile muf : uploadedFiles) {
                if (muf.getStatus() != MassUploadedFileStatus.OK) {
                    stepIDs.remove(muf.getStepId());
                }
            }

            // all others can be finished now
            for (Integer id : stepIDs) {
                Step so = StepManager.getStepById(id);
                if (so.getValidationPlugin() != null && so.getValidationPlugin().length() > 0) {
                    IValidatorPlugin ivp = (IValidatorPlugin) PluginLoader.getPluginByTitle(PluginType.Validation, so.getValidationPlugin());
                    ivp.setStep(so);
                    if (!ivp.validate()) {
                        log.error("Error while closing the step " + so.getTitel() + " for process " + so.getProzess().getTitel());
                        Helper.setFehlerMeldung("Error while closing the step " + so.getTitel() + " for process " + so.getProzess().getTitel());
                    }
                }
                Helper.addMessageToProcessLog(so.getProcessId(), LogType.DEBUG,
                        "Images uploaded and step " + so.getTitel() + " finished using Massupload Plugin.");
                HelperSchritte hs = new HelperSchritte();
                so.setBearbeitungsbenutzer(user);
                hs.CloseStepObjectAutomatic(so);
                finishedInserts.add(new MassUploadedProcess(so));
            }

            Helper.setMeldung("plugin_massupload_allFilesInserted");
        }
    }

    /**
     * check for uploaded file if a correct process can be found and assigned
     * 
     * @param uploadedFile
     */
    private void assignProcess(MassUploadedFile uploadedFile, Map<String, List<Process>> searchCache) {
        // get the relevant part of the file name
        String matchFile = uploadedFile.getFilename().substring(0, uploadedFile.getFilename().lastIndexOf("."));
        if (filenamePart.equals("prefix") && matchFile.contains(filenameSeparator)) {
            matchFile = matchFile.substring(0, matchFile.lastIndexOf(filenameSeparator));
        }
        if (filenamePart.equals("suffix") && matchFile.contains(filenameSeparator)) {
            matchFile = matchFile.substring(matchFile.lastIndexOf(filenameSeparator) + 1, matchFile.length());
        }

        // get all matching processes
        // first try to get this from the cache
        String filter = FilterHelper.criteriaBuilder(matchFile, false, null, null, null, true, false);
        List<Process> hitlist = searchCache == null ? null : searchCache.get(filter);
        if (hitlist == null) {
            // there was no result in the cache. Get result from the DB and then add it to the cache.
            hitlist = ProcessManager.getProcesses("prozesse.titel", filter, 0, 5);
            if (searchCache != null) {
                searchCache.put(filter, hitlist);
            }
        }

        // if list is empty
        if (hitlist == null || hitlist.isEmpty()) {
            uploadedFile.setStatus(MassUploadedFileStatus.ERROR);
            uploadedFile.setStatusmessage("No matching process found for this image.");
        } else {
            // if list is bigger then one hit
            if (hitlist.size() > 1) {
                String processtitles = "";
                for (Process process : hitlist) {
                    processtitles += process.getTitel() + ", ";
                }
                uploadedFile.setStatus(MassUploadedFileStatus.ERROR);
                uploadedFile.setStatusmessage("More than one matching process where found for this image: " + processtitles);
            } else {
                // we have just one hit and take it
                Process p = hitlist.get(0);
                uploadedFile.setProcessId(p.getId());
                uploadedFile.setProcessTitle(p.getTitel());
                try {
                    uploadedFile.setProcessFolder(p.getImagesOrigDirectory(true));
                } catch (IOException | InterruptedException | SwapException | DAOException e) {
                    uploadedFile.setStatus(MassUploadedFileStatus.ERROR);
                    uploadedFile.setStatusmessage("Error getting the master folder: " + e.getMessage());
                }

                if (uploadedFile.getStatus() != MassUploadedFileStatus.ERROR) {
                    // check if one of the open workflow steps is named as expected
                    boolean workflowStepAsExpected = false;

                    for (Step s : p.getSchritte()) {
                        if (s.getBearbeitungsstatusEnum() == StepStatus.OPEN) {
                            for (String st : stepTitles) {
                                if (st.equals(s.getTitel())) {
                                    workflowStepAsExpected = true;
                                    uploadedFile.setStepId(s.getId());
                                    stepIDs.add(s.getId());
                                }
                            }
                        }
                    }

                    // if correct open step was found, remember it
                    if (workflowStepAsExpected) {
                        uploadedFile.setStatus(MassUploadedFileStatus.OK);
                    } else {
                        uploadedFile.setStatus(MassUploadedFileStatus.ERROR);
                        uploadedFile.setStatusmessage(
                                "Process could be found, but there is no open workflow step with correct naming that could be accepted.");
                    }
                }
            }
        }
    }

}
