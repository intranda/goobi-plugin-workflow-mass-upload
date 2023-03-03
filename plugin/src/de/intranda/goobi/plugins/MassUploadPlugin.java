package de.intranda.goobi.plugins;

import java.awt.image.BufferedImage;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.imageio.ImageIO;

import org.apache.commons.configuration.XMLConfiguration;
import org.goobi.beans.Process;
import org.goobi.beans.Step;
import org.goobi.beans.User;
import org.goobi.goobiScript.GoobiScriptManager;
import org.goobi.goobiScript.GoobiScriptResult;
import org.goobi.managedbeans.LoginBean;
import org.goobi.production.enums.LogType;
import org.goobi.production.enums.PluginType;
import org.goobi.production.flow.statistics.hibernate.FilterHelper;
import org.goobi.production.plugin.PluginLoader;
import org.goobi.production.plugin.interfaces.IPlugin;
import org.goobi.production.plugin.interfaces.IValidatorPlugin;
import org.goobi.production.plugin.interfaces.IWorkflowPlugin;
import org.primefaces.event.FileUploadEvent;
import org.primefaces.model.file.UploadedFile;

import com.google.zxing.BarcodeFormat;
import com.google.zxing.BinaryBitmap;
import com.google.zxing.DecodeHintType;
import com.google.zxing.LuminanceSource;
import com.google.zxing.MultiFormatReader;
import com.google.zxing.NotFoundException;
import com.google.zxing.Result;
import com.google.zxing.client.j2se.BufferedImageLuminanceSource;
import com.google.zxing.common.HybridBinarizer;

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
import lombok.extern.log4j.Log4j2;
import net.xeoh.plugins.base.annotations.PluginImplementation;

@PluginImplementation
@Log4j2
@Data
public class MassUploadPlugin implements IWorkflowPlugin, IPlugin {

    private static final String PLUGIN_NAME = "intranda_workflow_massupload";
    private String allowedTypes;
    private String filenamePart;
    private String userFolderName;
    private String processTitleMatchType;
    private String filenameSeparator;
    //    private String processnamePart;
    //    private String processnameSeparator;
    private List<String> stepTitles;
    private List<MassUploadedFile> uploadedFiles = new ArrayList<>();
    private User user;
    private File tempFolder;
    private HashSet<Integer> stepIDs = new HashSet<>();
    private List<MassUploadedProcess> finishedInserts = new ArrayList<>();
    private boolean copyImagesViaGoobiScript = false;
    private boolean useBarcodes = false;
    private ExecutorService barcodePool;
    private volatile boolean analyzingBarcodes = false;
    private boolean currentlyInserting;
    private boolean hideInsertButtonAfterClick = false;

    /**
     * Constructor
     */
    public MassUploadPlugin() {
        log.info("Mass upload plugin started");
        XMLConfiguration config = ConfigPlugins.getPluginConfig(PLUGIN_NAME);
        allowedTypes = config.getString("allowed-file-extensions", "/(\\.|\\/)(gif|jpe?g|png|tiff?|jp2|pdf)$/");
        filenamePart = config.getString("filename-part", "prefix").toLowerCase();
        userFolderName = config.getString("user-folder-name", "mass_upload").toLowerCase();
        filenameSeparator = config.getString("filename-separator", "_").toLowerCase();
        //      processnamePart = ConfigPlugins.getPluginConfig(this).getString("processname-part", "complete").toLowerCase();
        //      processnameSeparator = ConfigPlugins.getPluginConfig(this).getString("processname-separator", "_").toLowerCase();
        stepTitles = Arrays.asList(config.getStringArray("allowed-step"));
        copyImagesViaGoobiScript = config.getBoolean("copy-images-using-goobiscript", false);
        useBarcodes = config.getBoolean("use-barcodes", false);
        if (useBarcodes) {
            barcodePool = Executors.newFixedThreadPool(2);
        }
        processTitleMatchType = config.getString("match-type", "contains");

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

    private void readUser() {
        if (user == null) {
            LoginBean login = Helper.getLoginBean();
            if (login != null) {
                user = login.getMyBenutzer();
            }
        }
    }

    /**
     * Handle the upload of a file
     * 
     * @param event
     */
    public void uploadFile(FileUploadEvent event) {
        try {
            if (tempFolder == null) {
                readUser();
                tempFolder = new File(ConfigurationHelper.getInstance().getTemporaryFolder(), user.getLogin());
                if (!tempFolder.exists()) {
                    if (!tempFolder.mkdirs()) {
                        throw new IOException("Upload folder for user could not be created: " + tempFolder.getAbsolutePath());
                    }
                }
            }
            UploadedFile upload = event.getFile();
            saveFileTemporary(upload.getFileName(), upload.getInputStream());
        } catch (IOException e) {
            log.error("Error while uploading files", e);
        }

    }

    public void sortFiles() {
        String currentBarcode = "";
        Map<String, List<Process>> searchCache = new HashMap<>();
        this.uploadedFiles.sort(Comparator.comparing(MassUploadedFile::getFilename));
        if (useBarcodes) {
            for (MassUploadedFile muf : this.uploadedFiles) {
                try {
                    if (!muf.isCheckedForBarcode()) {
                        String barcodeInfo = readBarcode(muf.getFile(), BarcodeFormat.CODE_128);
                        muf.setBarcodeValue(Optional.ofNullable(barcodeInfo));
                    }
                    if (muf.getBarcodeValue().isPresent()) {
                        currentBarcode = muf.getBarcodeValue().get();
                    }
                } catch (IOException e) {
                    log.error(e);
                }
                assignProcess(muf, searchCache, currentBarcode);
            }
        }
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
            readUser();
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
            if (useBarcodes) {
                Callable<String> readBarcodeTask = () -> {
                    return readBarcode(muf.getFile(), BarcodeFormat.CODE_128);
                };
                Future<String> futureBarcode = this.barcodePool.submit(readBarcodeTask);
                String barcodeInfo = null;
                try {
                    barcodeInfo = futureBarcode.get();
                    muf.setCheckedForBarcode(true);
                } catch (InterruptedException | ExecutionException e) {
                    log.error(e);
                }
                muf.setBarcodeValue(Optional.ofNullable(barcodeInfo));
            } else {
                assignProcessByFilename(muf, null);
            }
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
        finishedInserts = new ArrayList<>();
        stepIDs = new HashSet<>();
        try {
            readUser();
            File folder = new File(user.getHomeDir(), userFolderName);
            if (folder.exists() && folder.canRead()) {
                // we use the Files API intentionally, as we expect folders with many files in them.
                // The nio DirectoryStream initializes the Path objects lazily, so we don't have as many objects in memory and to create
                try (DirectoryStream<Path> files = Files.newDirectoryStream(folder.toPath())) {
                    Map<String, List<Process>> searchCache = new HashMap<>();
                    for (Path file : files) {
                        if (!Files.isDirectory(file) && !".DS_Store".equals(file.getFileName().toString())) {
                            MassUploadedFile muf = new MassUploadedFile(file.toFile(), file.getFileName().toString());
                            if (!useBarcodes) {
                                assignProcessByFilename(muf, searchCache);
                            }
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
        finishedInserts = new ArrayList<>();
        stepIDs = new HashSet<>();
    }

    /**
     * All uploaded files shall now be moved to the correct processes
     */
    public void startInserting() {

        this.hideInsertButtonAfterClick = true;
        if (this.currentlyInserting == true) {
            return;
        }

        readUser();

        try {
            this.currentlyInserting = true;

            if (copyImagesViaGoobiScript) {
                GoobiScriptCopyImages gsci = new GoobiScriptCopyImages();
                gsci.setUploadedFiles(uploadedFiles);
                gsci.setUser(user);
                List<GoobiScriptResult> goobiScriptResults = gsci.prepare(null, "copyFiles for mass upload", null);
                GoobiScriptManager gsm = Helper.getBeanByClass(GoobiScriptManager.class);
                gsm.enqueueScripts(goobiScriptResults);
                gsm.startWork();
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
        } finally {
            this.currentlyInserting = false;
        }
    }

    /**
     * check for uploaded file if a correct process can be found and assigned
     * 
     * @param uploadedFile
     */
    private void assignProcessByFilename(MassUploadedFile uploadedFile, Map<String, List<Process>> searchCache) {
        // get the relevant part of the file name
        String identifier = uploadedFile.getFilename().substring(0, uploadedFile.getFilename().lastIndexOf("."));
        if ("prefix".equals(filenamePart) && identifier.contains(filenameSeparator)) {
            identifier = identifier.substring(0, identifier.lastIndexOf(filenameSeparator));
        }
        if ("suffix".equals(filenamePart) && identifier.contains(filenameSeparator)) {
            identifier = identifier.substring(identifier.lastIndexOf(filenameSeparator) + 1, identifier.length());
        }

        assignProcess(uploadedFile, searchCache, identifier);
    }

    public void assignProcess(MassUploadedFile uploadedFile, Map<String, List<Process>> searchCache, String identifier) {
        // get all matching processes
        // first try to get this from the cache
        List<Process> hitlist = null;
        if ("exact".equals(processTitleMatchType)) {
            hitlist = searchCache == null ? null : searchCache.get(identifier);
            if (hitlist == null) {
                Process p = ProcessManager.getProcessByExactTitle(identifier);
                if (p != null) {
                    hitlist = new ArrayList<>();
                    hitlist.add(p);
                    if (searchCache != null) {
                        searchCache.put(identifier, hitlist);
                    }
                }
            }
        } else {
            String filter = FilterHelper.criteriaBuilder(identifier, false, null, null, null, true, false);
            hitlist = searchCache == null ? null : searchCache.get(filter);
            if (hitlist == null) {
                // there was no result in the cache. Get result from the DB and then add it to the cache.
                hitlist = ProcessManager.getProcesses("prozesse.titel", filter, 0, 5, null);
                if (searchCache != null) {
                    searchCache.put(filter, hitlist);
                }
            }
        }

        // if list is empty
        if (hitlist == null || hitlist.isEmpty()) {
            uploadedFile.setStatus(MassUploadedFileStatus.ERROR);
            uploadedFile.setStatusmessage("No matching process found for this image.");
        } else // if list is bigger then one hit
        if (hitlist.size() > 1) {
            StringBuilder processtitles = new StringBuilder();
            for (Process process : hitlist) {
                processtitles.append(process.getTitel()).append(", ");
            }
            uploadedFile.setStatus(MassUploadedFileStatus.ERROR);
            uploadedFile.setStatusmessage("More than one matching process where found for this image: " + processtitles.toString());
        } else {
            // we have just one hit and take it
            Process p = hitlist.get(0);
            uploadedFile.setProcessId(p.getId());
            uploadedFile.setProcessTitle(p.getTitel());
            try {
                uploadedFile.setProcessFolder(p.getImagesOrigDirectory(false));
            } catch (IOException | SwapException | DAOException e) {
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

    public boolean getShowInsertButton() {

        if (this.hideInsertButtonAfterClick) {
            return false;
        }

        if (currentlyInserting) {
            return false;

        }
        boolean showInsertButton =
                this.uploadedFiles.size() > 0 && this.uploadedFiles.stream().allMatch(muf -> muf.getStatus() != MassUploadedFileStatus.UNKNWON);
        return showInsertButton;
    }

    public boolean isShowInsertButton() {
        return getShowInsertButton();
    }

    public void assignProcessesWithBarcodeInfo() {
        this.analyzingBarcodes = true;
        Runnable myRunnable = () -> {
            String currentBarcode = "";
            Map<String, List<Process>> searchCache = new HashMap<>();
            this.uploadedFiles.sort(Comparator.comparing(MassUploadedFile::getFilename));
            for (MassUploadedFile muf : this.uploadedFiles) {
                try {
                    if (!muf.isCheckedForBarcode()) {
                        String barcodeInfo = readBarcode(muf.getFile(), BarcodeFormat.CODE_128);
                        muf.setBarcodeValue(Optional.ofNullable(barcodeInfo));
                    }
                    if (muf.getBarcodeValue().isPresent()) {
                        currentBarcode = muf.getBarcodeValue().get();
                    }
                } catch (IOException e) {
                    log.error(e);
                }
                assignProcess(muf, searchCache, currentBarcode);
            }
            this.analyzingBarcodes = false;
        };
        new Thread(myRunnable).start();
    }

    private static String readBarcode(File inputFile, BarcodeFormat format) throws IOException {
        BufferedImage bufferedImage = ImageIO.read(inputFile);
        LuminanceSource source = new BufferedImageLuminanceSource(bufferedImage);
        BinaryBitmap bitmap = new BinaryBitmap(new HybridBinarizer(source));
        try {
            Map<DecodeHintType, Boolean> hints = new TreeMap<>();
            hints.put(DecodeHintType.TRY_HARDER, Boolean.TRUE);
            Result result = new MultiFormatReader().decode(bitmap, hints);
            if (result.getBarcodeFormat() == format) {
                return result.getText();
            }
        } catch (NotFoundException e) {
            log.debug("There is no QR code in the image {}", inputFile.getName());
        }
        return null;
    }

}
