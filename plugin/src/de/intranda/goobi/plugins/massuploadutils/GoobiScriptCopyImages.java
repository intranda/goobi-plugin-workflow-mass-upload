package de.intranda.goobi.plugins.massuploadutils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.goobi.beans.Step;
import org.goobi.beans.User;
import org.goobi.goobiScript.AbstractIGoobiScript;
import org.goobi.goobiScript.GoobiScriptResult;
import org.goobi.goobiScript.IGoobiScript;
import org.goobi.managedbeans.LoginBean;
import org.goobi.production.enums.GoobiScriptResultType;
import org.goobi.production.enums.LogType;
import org.goobi.production.enums.PluginType;
import org.goobi.production.plugin.PluginLoader;
import org.goobi.production.plugin.interfaces.IValidatorPlugin;

import de.sub.goobi.helper.Helper;
import de.sub.goobi.helper.HelperSchritte;
import de.sub.goobi.helper.StorageProvider;
import de.sub.goobi.persistence.managers.StepManager;

public class GoobiScriptCopyImages extends AbstractIGoobiScript implements IGoobiScript {
    private static final Logger logger = Logger.getLogger(GoobiScriptCopyImages.class);

    private User user;
    private List<MassUploadedFile> uploadedFiles = new ArrayList<MassUploadedFile>();

    public void setUser(User user) {
        this.user = user;
    }

    public void setUploadedFiles(List<MassUploadedFile> uploadedFiles) {
        this.uploadedFiles = uploadedFiles;

    }

    @Override
    public List<GoobiScriptResult> prepare(List<Integer> processes, String command, Map<String, String> parameters) {
        List<GoobiScriptResult> newList = new ArrayList<>();
        starttime = (int) System.currentTimeMillis() / 1000;
        LoginBean login = Helper.getLoginBean();
        username = login.getMyBenutzer().getNachVorname();
        int count = 0;
        for (MassUploadedFile muf : uploadedFiles) {
            if (muf.getStatus() == MassUploadedFileStatus.OK) {
                muf.setTransfered(false);
                Map<String, String> mufParams = new LinkedHashMap<String, String>();
                mufParams.put("uploadFileIndex", Integer.toString(count));
                mufParams.put("filename", muf.getFilename());
                GoobiScriptResult gsr = new GoobiScriptResult(muf.getProcessId(), command, mufParams, username, starttime);
                gsr.setCustomGoobiScriptImpl(this);
                gsr.setProcessTitle(muf.getProcessTitle());
                newList.add(gsr);
            } else {
                Helper.setFehlerMeldung("File could not be matched and gets skipped: " + muf.getFilename());
            }
            count++;
        }
        return newList;
    }

    @Override
    public void execute(GoobiScriptResult gsr) {
        // execute all jobs that are still in waiting state
        gsr.updateTimestamp();
        Integer uploadFileIndex = Integer.parseInt(gsr.getParameters().get("uploadFileIndex"));
        MassUploadedFile muf = uploadedFiles.get(uploadFileIndex);
        if (muf.getStatus() == MassUploadedFileStatus.OK) {
            Path src = Paths.get(muf.getFile().getAbsolutePath());
            Path target = Paths.get(muf.getProcessFolder(), muf.getFilename());
            try {
                StorageProvider.getInstance().copyFile(src, target);
            } catch (IOException e) {
                muf.setStatus(MassUploadedFileStatus.ERROR);
                muf.setStatusmessage("File could not be copied to: " + target.toString());
                logger.error("Error while copying file during mass upload goobiscript", e);
                Helper.setFehlerMeldung("Error while copying file during mass upload goobiscript", e);
            }
            muf.getFile().delete();
            muf.setTransfered(true);
        }

        if (muf.getStatus() == MassUploadedFileStatus.OK) {
            Step so = StepManager.getStepById(muf.getStepId());
            if (so.getValidationPlugin() != null && so.getValidationPlugin().length() > 0) {
                IValidatorPlugin ivp = (IValidatorPlugin) PluginLoader.getPluginByTitle(PluginType.Validation, so.getValidationPlugin());
                ivp.setStep(so);
                if (!ivp.validate()) {
                    gsr.setResultMessage("Images copied, but validation not successful.");
                    gsr.setResultType(GoobiScriptResultType.ERROR);
                } else {
                    gsr.setResultMessage("Images copied and validated successfully.");
                    gsr.setResultType(GoobiScriptResultType.OK);
                }
            } else {
                gsr.setResultMessage("Images copied successfully.");
                gsr.setResultType(GoobiScriptResultType.OK);
            }
            gsr.updateTimestamp();

            if (gsr.getResultType() == GoobiScriptResultType.OK && isLastFileOfProcess(muf)) {
                Helper.addMessageToProcessLog(so.getProcessId(), LogType.DEBUG,
                        "Image uploaded and step " + so.getTitel() + " finished using Massupload Plugin via Goobiscript.");
                HelperSchritte hs = new HelperSchritte();
                so.setBearbeitungsbenutzer(user);
                hs.CloseStepObjectAutomatic(so);
            }
        } else {
            gsr.setResultMessage("The file does not exist or is not accessible.");
            gsr.setResultType(GoobiScriptResultType.ERROR);
        }
    }

    /**
     * Check if there are other MassUploadedFiles in the list which are still not processed
     * 
     * @param muf
     * @return
     */
    private boolean isLastFileOfProcess(MassUploadedFile muf) {
        for (MassUploadedFile m : uploadedFiles) {
            if (m.getProcessTitle() != null && m.getProcessTitle().equals(muf.getProcessTitle()) && m.isTransfered() == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isVisible() {
        return false;
    }

    @Override
    public String getAction() {
        // TODO Auto-generated method stub
        return "massuploadCopyImages";
    }

}
