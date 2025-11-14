package de.intranda.goobi.plugins.massuploadutils;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.goobi.production.properties.DisplayProperty;

import java.util.List;

@Data
@AllArgsConstructor
public class MassUploadProfile {
    private String name;
    private String allowedTypes;
    private String userFolderName;
    private String detectionType;
    private boolean copyImagesViaGoobiScript;
    private boolean instantMove;
    private List<String> stepTitles;
    private String filenamePart;
    private String filenameSeparator;
    private String processTitleMatchType;
    private List<PropertyValue> propertiesToSet;
}
