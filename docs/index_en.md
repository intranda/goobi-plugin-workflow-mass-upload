---
title: Mass upload
identifier: intranda_workflow_massupload
description: This workflow plugin allows a mass upload of files with automatically assigned to the correct Goobi processes either on the basis of the file file names or on the basis of the analysed barcodes.
published: true
---

## Introduction
This workflow plugin allows the mass upload of files that are to be automatically assigned to the correct processes by Goobi workflow. The plugin provides its own interface for this purpose, which either allows uploading directly via the web interface or, alternatively, can also read images from the user directory. These images are checked by the plugin for their name in order to determine the corresponding Goobi process. If the process can be identified unambiguously and the Goobi process identified is also in the configured workflow step, the images are assigned to this step and the workflow is processed further.

In addition to assigning the files based on the file name, it is also possible to configure an image analysis that reads barcodes instead. This makes it possible for images of several processes to be named consecutively, for example, and only a separator sheet needs to be included between the images. All images after such a separator sheet with an identifiable barcode are assigned to the respective process until the next barcode in the file stack is determined.

## Installation
To install the plugin, the following two files must be installed:

```bash
/opt/digiverso/goobi/plugins/workflow/plugin_intranda_workflow_massupload.jar
/opt/digiverso/goobi/plugins/GUI/plugin_intranda_workflow_massupload-GUI.jar
```

To configure how the plugin should behave, various values can be adjusted in the configuration file. The configuration file is usually located here:

```bash
/opt/digiverso/goobi/config/plugin_intranda_workflow_massupload.xml
```

To use this plugin, the user must have the correct role authorisation.

![The plugin cannot be used without correct authorisation](screen1_en.png)

Therefore, please assign the role `Plugin_Goobi_Massupload` to the group.

![Correctly assigned role for users](screen2_en.png)


## Overview and functionality
If the plugin has been installed and configured correctly, it can be found under the 'Workflow' menu item.

![Open plugin for the upload](screen3_en.png)

At this point, files can either be uploaded or read from the user directory. After analysing the file names or images, Goobi workflow shows which processes the uploaded images can be assigned to.

![Analysed files with display of the associated processes](screen4_en.png)

Click on the `Import files into processes` button to move the images to the directories of the recognised processes and continue the workflow.

Please note: If barcodes are to be recognised in order to determine the processes, it is important that the barcodes are also available in sufficient size and quality for the recognition to be successful.

![Digitised separator sheets with barcodes for automatic recognition](screen5_en.png)


## Configuration
The plugin is configured in the file `plugin_intranda_workflow_massupload.xml` as shown here:

{{CONFIG_CONTENT}}

Parameter                       | Explanation
--------------------------------|----------------------------------------
`allowed-file-extensions`       | This parameter is used to specify which files may be uploaded. This is a regular expression.
`user-folder-name`              | If the files are to be read from the user directory, the name of the folder within the user directory from which the files are to be read can be specified here.
`detection-type`                | This parameter is used to specify whether the assignment to the processes should be based on barcodes or whether it should be based on the file names. The values available here are `filename` for the use of file names and `barcode` for barcode recognition. If the value " `user` is specified, the user is given a selection option in the user interface.
`copy-images-using-goobiscript` | If data transfer is to take place in the background using the GoobiScript queue functionality, this can be specified here.
`allowed-step`                  | Use this repeatable parameter to specify which work step in the determined process must currently be in the `open` status.
`filename-part`                 | This parameter can be used to specify how the file names are to be assigned to the processes.
`filename-separator`            | Specify the separator that is to be used to truncate a prefix or suffix. This allows you to specify that, for example, an operation called `kleiuniv_987654321` is to be determined from a file named `kleiuniv_987654321_00002.tif` when an assignment is made using `prefix` and the separator `_`.
`match-type`                    | Specify here whether the matching of the processes should be carried out using `exact` via an exact name or using `contains` whether the process name should only contain the value.
