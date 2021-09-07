# Dremio Cloner

Dremio Cloner is a python-based utility for Dremio Enterprise. It supports the following commands: get, put, cascade-acl, report-acl, report-reflections.

Dremio Cloner can be utilized for: 
- Migrating entire dremio environments, for example, from community edition to enterprise edition
- CI/CD processes
- Disaster Recovery scenarios
- Partial backup/restore
- Security Audit reporting
- Reflection reporting

Dremio cloner is executied with the following command:
`python dremio_cloner.py [config_file.json]`

Dremio Cloner can be run on Python 2.7 and 3.

## Command &quot;get&quot;

Command &quot;get&quot; selectively saves definitions for objects such as Source, Space, Folder, PDS, VDS, ACLs, Reflections, Queues, Rules, Tags, Wikis, and Votes from a Dremio environment into a JSON file.

The command is configured with a JSON file with configuration attributes listed below. For detailed description of the configuration JSON attributes, see Reference section below in Appendix 1.

- &quot;command&quot;:&quot;get&quot;
- &quot;source&quot;: defines source Dremio Environment with
  - &quot;endpoint&quot;
  - &quot;username&quot;
  - &quot;password&quot;
  - &quot;verify\_ssl&quot;
  - &quot;is\_community\_edition&quot;
  - &quot;graph\_api\_support&quot;
  - &quot;is\_dremio\_cloud&quot;
  - &quot;dremio\_cloud\_org\_id&quot;
  - &quot;dremio\_cloud\_project\_id&quot;
- &quot;target&quot;: defines an output filename or a directory with
  - &quot;filename&quot;
  - &quot;directory&quot;
  - &quot;overwrite&quot;
- &quot;options&quot;:
  - logging options 
    - &quot;logging.level&quot;
    - &quot;logging.format&quot;
    - &quot;logging.filename&quot;
    - &quot;logging.verbose&quot;
  - miscellaneous options 
    - &quot;max\_errors&quot;
    - &quot;http\_timeout&quot;
  - scope of _Space_ processing 
    - &quot;space.process\_mode&quot;
    - &quot;folder.process\_mode&quot;
    - &quot;space.filter&quot;
    - &quot;space.exclude.filter&quot;
    - &quot;space.folder.filter&quot;
    - &quot;space.folder.exclude.filter&quot;
  - scope of _Source_ processing 
    - &quot;source.process\_mode&quot;
    - &quot;source.filter&quot;
    - &quot;source.exclude.filter&quot;
    - &quot;source.folder.filter&quot;
    - &quot;source.folder.exclude.filter&quot;
  - scope of _PDS_ processing 
    - &quot;pds.process\_mode&quot;
    - &quot;pds.filter&quot;
    - &quot;pds.exclude.filter
    - &quot;pds.list.useapi&quot;
  - scope of _VDS_ processing 
    - &quot;vds.process\_mode&quot;
    - &quot;vds.filter&quot;
    - &quot;vds.exclude.filter&quot;
    - &quot; vds.dependencies.process\_mode&quot;
  - scope of _Reflection_ processing 
    - &quot;reflection.process\_mode&quot;
	- &quot;reflection.id\_include\_list&quot;
  - scope of _Workload Management_ processing
    - &quot;wlm.queue.process\_mode&quot;
    - &quot;wlm.rule.process\_mode&quot;
  - scope of processing other objects
    - &quot;user.process\_mode&quot;
    - &quot;group.process\_mode&quot;
    - &quot;wiki.process\_mode&quot;
    - &quot;tag.process\_mode&quot;
    - &quot;home.process\_mode&quot;
    - &quot;vote.process\_mode&quot;

Please see a sample JSON configuration file in the config folder of this repository.

## Command &quot;put&quot;

Command &quot;put&quot; selectively updates an existing Dremio Environment from a JSON file previously generated by &quot;get&quot; command.

Command &quot;put&quot; can also process ACL transformation. For example, it can transform ACLs to use LDAP_GROUP_PROD instead of LDAP_GROUP_DEV.

The command is configured with a JSON file with configuration attributes listed below. For detailed description of the configuration JSON attributes, see Reference section below in Appendix 1.

- &quot;command&quot;:&quot;put&quot;
- &quot;source&quot;: defines an output filename with
  - &quot;filename&quot;
  - &quot;directory&quot;
- &quot;target&quot;: defines target Dremio Environment with
  - &quot;endpoint&quot;
  - &quot;username&quot;
  - &quot;password&quot;
  - &quot;verify\_ssl&quot;
  - &quot;is\_community\_edition&quot;
  - &quot;is\_dremio\_cloud&quot;
  - &quot;dremio\_cloud\_org\_id&quot;
  - &quot;dremio\_cloud\_project\_id&quot;
- &quot;options&quot;:
  - logging options 
    - &quot;logging.level&quot;
    - &quot;logging.format&quot;
    - &quot;logging.filename
    - &quot;logging.verbose&quot;
  - miscellaneous options 
    - &quot;max\_errors&quot;
    - &quot;http\_timeout&quot;
    - &quot;source.retry\_timedout&quot;
    - &quot;dry\_run&quot;
  - processing of _User_ and _Group_ objects missing in the target environemnt
    - &quot;space.ignore\_missing\_acl\_user&quot;
    - &quot;space.ignore\_missing\_acl\_group&quot;
    - &quot;folder.ignore\_missing\_acl\_user&quot;
    - &quot;folder.ignore\_missing\_acl\_group&quot;
    - &quot;source.ignore\_missing\_acl\_user&quot;
    - &quot;source.ignore\_missing\_acl\_group&quot;
    - &quot;pds.ignore\_missing\_acl\_user&quot;
    - &quot;pds.ignore\_missing\_acl\_group&quot;
    - &quot;vds.ignore\_missing\_acl\_user&quot;
    - &quot;vds.ignore\_missing\_acl\_group&quot;
  - scope of _Space_ processing 
    - &quot;space.process\_mode&quot;
    - &quot;space.filter&quot;
    - &quot;space.exclude.filter&quot;
    - &quot;folder.process\_mode&quot;
    - &quot;space.folder.filter&quot;
    - &quot;space.folder.exclude.filter&quot;
  - scope of _Source_ processing 
    - &quot;source.process\_mode&quot;
    - &quot;source.filter&quot;
    - &quot;source.exclude.filter&quot;
    - &quot;source.folder.filter&quot;
    - &quot;source.folder.exclude.filter&quot;
  - scope of _PDS_ processing 
    - &quot;pds.process\_mode&quot;
    - &quot;pds.filter&quot;
    - &quot;pds.exclude.filter
    - &quot;pds.list.useapi&quot;
  - scope of _VDS_ processing 
    - &quot;vds.process\_mode&quot;
    - &quot;vds.filter&quot;
    - &quot;vds.exclude.filter&quot;
    - &quot;vds.max\_hierarchy\_depth&quot;
  - scope of _Reflection_ processing 
    - &quot;reflection.process\_mode&quot;
    - &quot;pds.reflection\_refresh\_mode&quot;
  - scope of processing other objects
    - &quot;user.process\_mode&quot;
    - &quot;group.process\_mode&quot;
    - &quot;wiki.process\_mode&quot;
    - &quot;tag.process\_mode&quot;
    - &quot;home.process\_mode&quot;
    - &quot;vote.process\_mode&quot;
  - acl transformation processing 
    - &quot;transformation&quot;
      - &quot;acl&quot;
        - &quot;file&quot;


Please see a sample JSON configuration file in the config folder if this repository.

## Command &quot;cascade-acl&quot;

Command &quot;cascade-acl&quot; selectively propagates ACLs in an object hierarchy.

The command is configured with a JSON file with configuration attributes listed below. For detailed description of the configuration JSON attributes, see Reference section below in Appendix 1.

- &quot;command&quot;:&quot;cascade-acl&quot;
- &quot;target&quot;: defines Dremio Environment to be processed with
  - &quot;endpoint&quot;
  - &quot;username&quot;
  - &quot;password&quot;
  - &quot;verify\_ssl&quot;
- &quot;options&quot;:
  - logging options
    - &quot;logging.level&quot;
    - &quot;logging.format&quot;
    - &quot;logging.filename
    - &quot;logging.verbose&quot;
  - miscellaneous options
    - &quot;max\_errors&quot;
    - &quot;http\_timeout&quot;
    - &quot;source.retry\_timedout&quot;
    - &quot;dry\_run&quot;
  - scope of _Space_ processing 
    - &quot;space.filter&quot;
    - &quot;space.exclude.filter&quot;
    - &quot;space.cascade-acl-origin.override-object&quot;
    - &quot;space.folder.filter&quot;
    - &quot;space.folder.exclude.filter&quot;
    - &quot;space.folder.cascade-acl-origin.filter&quot;
  - scope of _Source_ processing
    - &quot;source.filter&quot;
    - &quot;source.exclude.filter&quot;
    - &quot;source.cascade-acl-origin.override-object&quot;
    - &quot;source.folder.filter&quot;
    - &quot;source.folder.exclude.filter&quot;
  - scope of _PDS_ processing 
    - &quot;pds.filter&quot;
    - &quot;pds.exclude.filter
    - &quot;pds.list.useapi&quot;
  - scope of _VDS_ processing 
    - &quot;vds.filter&quot;
    - &quot;vds.exclude.filter&quot;

Note, if none of _space.cascade-acl-origin.override-object_, _space.folder.cascade-acl-origin.filter_, and _source.cascade-acl-origin.override-object_ specified:

- each Space ACL will be propagated through its hierarchy and applied to Folders and VDSs as per filter configuration
    - To cascade ACLs for all spaces, specify `{"space.filter": "*"}`
    - To omit cascading any space ACLs, specify `{"space.filter": ""}`
    - To cascade ACLs for a specific named space, specify `{"space.filter": "spacename"}` where `spacename` should be replaced with the actual name of the space
- each Source ACL will be propagated through its hierarchy and applied to PDSs as per filter configuration
    - To cascade ACLs for all sources, specify `{"source.filter": "*"}`
    - To omit cascading any source ACLs, specify `{"source.filter": ""}`
    - To cascade ACLs for a specific named source, specify `{"source.filter": "sourcename"}` where `sourcename` should be replaced with the actual name of the source

Please see a sample JSON configuration file in the config folder of this repository.



## Command &quot;report-acl&quot;

Command &quot;report-acl&quot; produces a selective security report on all objects with ACL in a Dremio environment.

The command is configured with a JSON file with configuration attributes listed below. For detailed description of the configuration JSON attributes, see Reference section below in Appendix 1.

- &quot;command&quot;:&quot;report-acl&quot;
- &quot;source&quot;: defines Dremio Environment with
  - &quot;endpoint&quot;
  - &quot;username&quot;
  - &quot;password&quot;
  - &quot;verify\_ssl&quot;
  - &quot;is\_rbac\_version&quot;
- &quot;target&quot;: defines an output filename with
  - &quot;filename&quot;
- &quot;options&quot;:
  - logging options 
    - &quot;logging.level&quot;
    - &quot;logging.format&quot;
    - &quot;logging.filename
    - &quot;logging.verbose&quot;
  - miscellaneous options 
    - &quot;max\_errors&quot;
    - &quot;http\_timeout&quot;
    - &quot;source.retry\_timedout&quot;
  - report format
    - &quot;report.csv.delimiter&quot;
    - &quot;report.csv.newline&quot;
  - scope of Space processing 
    - &quot;space.filter&quot;
    - &quot;space.exclude.filter&quot;
    - &quot;space.folder.filter&quot;
    - &quot;space.folder.exclude.filter&quot;
  - scope of Source processing 
    - &quot;source.filter&quot;
    - &quot;source.exclude.filter&quot;
    - &quot;source.folder.filter&quot;
    - &quot;source.folder.exclude.filter&quot;
  - scope of PDS processing 
    - &quot;pds.filter&quot;
    - &quot;pds.exclude.filter
    - &quot;pds.list.useapi&quot;
  - scope of VDS processing 
    - &quot;vds.filter&quot;
    - &quot;vds.exclude.filter&quot;

Please see a sample JSON configuration file in the config folder of this repository.




## Command &quot;report-reflections&quot;

Command &quot;report-reflections&quot; produces a reflection report with reflection usage information and ranking on potentially duplicate reflections.

The command is configured with a JSON file with configuration attributes listed below. For detailed description of the configuration JSON attributes, see Reference section below in Appendix 1.

- &quot;command&quot;:&quot;report-acl&quot;
- &quot;source&quot;: defines Dremio Environment with
  - &quot;endpoint&quot;
  - &quot;username&quot;
  - &quot;password&quot;
  - &quot;verify\_ssl&quot;
- &quot;target&quot;: defines an output filename with
  - &quot;filename&quot;
- &quot;options&quot;:
  - logging options 
    - &quot;logging.level&quot;
    - &quot;logging.format&quot;
    - &quot;logging.filename
    - &quot;logging.verbose&quot;
  - miscellaneous options
    - &quot;max\_errors&quot;
    - &quot;http\_timeout&quot;
    - &quot;source.retry\_timedout&quot;
  - report format
    - &quot;report.csv.delimiter&quot;
    - &quot;report.csv.newline&quot;

Note, that this command does not provide any option for Scope definition. Please see a sample JSON configuration file in the config folder of this repository.


## Configuration Options

### _Target_ or _Source_ section, when Dremio Environment definition is required.

| **Configuration Option** | **Description** |
| --- | --- |
| endpoint | Defines Dremio API endpoint. For example, [http://localhost:9047/](http://localhost:9047/). Mandatory attribute. |
| username | Dremio user name. Must be an Admin. Mandatory attribute. |
| password | Dremio user password. Optional field. If not provided, CLI will request password at runtime. |
| verify\_ssl | If set to False, Dremio Cloner will not validate SSL certificate of the Dremio Environment. Default is True. |
| is\_community\_edition | Set to True if reading Dremio CE. Writing to Dremio CE is not supported. |
| graph\_api\_support | Dremio Graph API is only available in EE starting version 4.0.0. Default value is False. |
| is\_rbac\_version | Set to True if the version of Dremio EE supports the RBAC privileges model. Default value is False. |
| is\_dremio\_cloud | Set to True if reading from or writing to Dremio Cloud. Default value is False. |
| dremio\_cloud\_org\_id | Dremio Cloud Organization ID to connect to. |
| dremio\_cloud\_project\_id | Dremio Cloud Project ID to connect to. |

### _Target_ or _source_ section, when defined with a file name

| **Configuration Option** | **Description** |
| --- | --- |
| filename | Defines a JSON filename to be used as either source of information for **put** command or target for saving data for **get** command. The JSON file will encapsulate entire information on a Dremio environment. Either _filename_ or _directory_ must be defined.|
| directory | Similar to filename above. However, a folder structure, identical to Dremio environment will be created and the information on a Dremio objects will be stored in sepearate files within this folder structure. This option allows for use cases with indivudal processing of Dremio objects by external tools, such as github. |
| overwrite | Allows to overwrite existing JSON file or directory. |

### Logging options

| **Configuration Option** | **Description** |
| --- | --- |
| logging.level | Defines logging level: DEBUG, INFO, WARN, ERROR |
| logging.format | Logging format. For example: &quot;%(levelname)s:%(asctime)s:%(message)s&quot; |
| logging.filename | Filename for logging. File will be appended if exists. If this option is omitted, standard output will be used for logging. |
| logging.verbose | Default False. Produce verbose logging such as log entire entity definitions if set to True. |

### Miscellaneous options

| **Configuration Option** | **Description** |
| --- | --- |
| max\_errors | Defines a number of errors at which processing will be terminated. |
| http\_timeout | Timeout for each API call. This parameter might become important in certain situations when Sources defined in Dremio are not available.   |
| dry\_run | Defines a Dremio Cloner execution that will not update a target Dremio environment. In conjunction with logging.level set to WARN allows to execute Dremio Cloner without an impact on the target environment and check the log file for all activities that would have been submitted to the target Dremio Environment. Respective log entries will include _dry_run_ keyword. |
| vds.\_max\_hierarchy\_depth | Defines maximum level of VDS hierarchy supported by Dremio Cloner. It&#39;s a guard rail with default value of 100. |

### Scope of Dremio Space processing

| **Configuration Option** | **Description** |
| --- | --- |
| space.filter | A filter that defines what Spaces will be **included** into processing. &quot;\*&quot; will include all Spaces. Empty field will exclude all Spaces. Star may be used multiple times in the filter to define a pattern. Folders must be separated with backslash. Works in logical AND with _space.exclude.filter_. |
| space.exclude.filter | A filter that defines what Spaces will be **excluded** into processing. &quot;\*&quot; will exclude all Spaces. Empty field will include all Spaces. Star may be used multiple times in the filter to define a pattern. Folders must be separated with backslash. Works in logical AND with _space.filter_. |
| space.folder.filter | A filter that defines what Space Folders will be **included** into processing. &quot;\*&quot; will include all Folders. Empty field will exclude all Folders. Star may be used multiple times in the filter to define a pattern. Folders must be separated with backslash. Works in logical AND with _space.folder_._exclude_._filter_. |
| space.folder.exclude.filter | A filter that defines what Space Folders will be **excluded** into processing. &quot;\*&quot; will exclude all Folders. Empty field will include all Spaces. Star may be used multiple times in the filter to define a pattern. Folders must be separated with backslash. Works in logical AND with _space.folder_._filter_. |

### Scope of Dremio Source processing

| **Configuration Option** | **Description** |
| --- | --- |
| source.filter | A filter that defines what Sources will be **included** into processing. &quot;\*&quot; will include all Sources. Empty field will exclude all Sources. Star may be used multiple times in the filter to define a pattern. Folders must be separated with backslash. Works in logical AND with _source.exclude.filter_. |
| source.exclude.filter | A filter that defines what Spaces will be **excluded** into processing. &quot;\*&quot; will exclude all Spaces. Empty field will include all Sources. Star may be used multiple times in the filter to define a pattern. Folders must be separated with backslash. Works in logical AND with _source.filter_. |
| source.folder.filter | A filter that defines what Source Folders will be **included** into processing. &quot;\*&quot; will include all Folders. Empty field will exclude all Folders. Star may be used multiple times in the filter to define a pattern. Folders must be separated with backslash. Works in logical AND with _source.exclude.filter_. |
| source.folder.exclude.filter | A filter that defines what Source Folders will be **excluded** into processing. &quot;\*&quot; will exclude all Folders. Empty field will include all Spaces. Star may be used multiple times in the filter to define a pattern. Folders must be separated with backslash. Works in logical AND with _source.filter_. |

### Scope of Dremio PDS processing

| **Configuration Option** | **Description** |
| --- | --- |
| pds.filter | A filter that defines what PDSs will be **included** into processing. &quot;\*&quot; will include all PDSs. Empty field will exclude all PDSs. Star may be used multiple times in the filter to define a pattern. Folders must be separated with backslash. Works in logical AND with _pds.exclude.filter_. |
| pds.exclude.filter | A filter that defines what PDSs will be **excluded** into processing. &quot;\*&quot; will exclude all PDSs. Empty field will include all PDSs. Star may be used multiple times in the filter to define a pattern. Folders must be separated with backslash. Works in logical AND with _pds.filter_. |
| pds.list.useapi | Forces to use API for collecting list of PDSs if set to True. Default value is False which means that INFOMRATION\_SCHEMA will be utilized instead of API. False is a recommended value. |

### Scope of Dremio VDS processing

| **Configuration Option** | **Description** |
| --- | --- |
| vds.filter | A filter that defines what VDSs will be **included** into processing. &quot;\*&quot; will include all VDSs. Empty field will exclude all VDSs. Star may be used multiple times in the filter to define a pattern. Folders must be separated with backslash. Works in logical AND with _vds.exclude.filter_. |
| vds.exclude.filter | A filter that defines what VDSs will be **excluded** into processing. &quot;\*&quot; will exclude all VDSs. Empty field will include all VDSs. Star may be used multiple times in the filter to define a pattern. Folders must be separated with backslash. Works in logical AND with _vds.filter_. |

### Scope of Dremio Reflection processing

| **Configuration Option** | **Description** |
| --- | --- |
| reflection.id\_include\_list | If specified, a list filter that defines what reflection ids will be **included** into processing during &quot;get&quot; command execution. If this option is not specified or the list is empty then GET will include all reflections, which is the default behavior. Example: `{"reflection.id_include_list": ["dc86ab2e-8ebf-4d69-9302-911875a79e74", "ad3444df-7da5-4ea5-9624-b7705f351914"]}` |

### Scope of User and Group processing

| **Configuration Option** | **Description** |
| --- | --- |
| user.process\_mode group.process\_mode | Determines if users will be created in the target Dremio Environment if they are referenced in the source JSON file but not in the target environment. Applicable for &quot;put&quot; command only. However, user creation is not possible with the current Dremio API. This parameter can only have a single value _skip_. |
| space.ignore\_missing\_acl\_user space.ignore\_missing\_acl\_group folder.ignore\_missing\_acl\_user folder.ignore\_missing\_acl\_group source.ignore\_missing\_acl\_user source.ignore\_missing\_acl\_group pds.ignore\_missing\_acl\_user pds.ignore\_missing\_acl\_group vds.ignore\_missing\_acl\_user vds.ignore\_missing\_acl\_group | These configuration parameters define if Dremio Cloner ignores a situation when a user or a group is defined in an ACL in the source JSON file but is not present in the target Dremio Environment. This situation is a potential security risk as an ACL may be created with no limitations in the target environment when all referenced users and groups cannot be found. Default value is False. |

### Scope of object-level processing

| **Configuration Option** | **Description** |
| --- | --- |
| space.process\_mode folder.process\_mode source.process\_mode pds.process\_mode vds.process\_mode reflection.process\_mode pds.reflection\_refresh\_mode wlm.queue.process\_mode wlm.rule.process\_mode wiki.process\_mode tag.process\_mode home.process\_mode vote.process\_mode | Defines whether Dremio Cloner will 1) insert new objects only or 2) update existing objects only or 3) do an upsert. These parameters can be set to: _skip_, _create_only_, _update_only_, _create_overwrite_, _process_. _process_ is only aplicable for &quot;get&quot; command.  _skip_ will prevent any changes to the target Dremio Environment for the specified object type.  Note, _pds.process_mode_ can only take _skip_ and _promote_ with _promote_ updating PDS ACL as required. |
| vds.dependencies.process\_mode | Possible values: _ignore_, _get_. Default _ignore_. If set to _get_, Dremio Cloner  will collect information on all decencies throughout the object hierarchy (VDS and PDS) required for each VDS that satisfies VDS filter criteria. |

### Cascade-acl specific parameters

| **Configuration Option** | **Description** |
| --- | --- |
| space.cascade-acl-origin.override-object | If specified, overrides default behavior for Space hierarchy and an ACL of the object specified in this parameter will be used through **all Spaces all hierarchies** instead of the respective Spaces&#39; ACLs. A valid example is this: `{"space.filter": "spacetest"}, {"space.cascade-acl-origin.override-object": "spacetest/spacetest_folder"},` which is interpreted as read the ACLs from the object called spacetest/spacetesttest_folder and apply those ACLs to each object under the space called spacetest.|
| source.cascade-acl-origin.override-object | If specified, overrides default behavior for Source hierarchy and an ACL of the object specified in this parameter will be used through **all Source all hierarchies** instead of the respective Sources&#39; ACLs. |
| space.folder.cascade-acl-origin.filter | If specified, overrides default behavior for Space hierarchy and an ACLs of the Folders selected by this will be used through **its Folder hierarchy** instead of the respective Source&#39;s ACL. A valid example is this: `{"space.filter": "spacetest"}, {"space.cascade-acl-origin.override-object": "spacetest/spacetest_folder"}, {"space.folder.cascade-acl-origin.filter": "another_folder"},` which can be interpreted as all objects under spacetest will get the ACLs that are defined in spacetest/spacetest_folder, EXCEPT for those in spacetest/another_folder. All objects beneath another_folder (whose full path is spacetest/another_folder in this example) will have their ACLs set to whatever the ACLs are on another_folder.|

### Transformation parameters

| **Configuration Option** | **Description** |
| --- | --- |
| transformation | If specified, allows for transformation during &quot;put&quot; command execution. Supported transformation is ACL transofrmation. Transformation rules are specified in a separate json file and the file is referenced in the main comnfiguration file. For example: `{"transformation": {"acl": {"file": "acl_transformation.json"}}}` |

### Report format parameters

| **Configuration Option** | **Description** |
| --- | --- |
| report.csv.delimiter | A field delimiter used to generate a report. |
| report.csv.newline | A new line delimiter used to generate a report. |
