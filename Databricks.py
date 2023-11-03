#Data Bricks Features
    model training 
    model serving

# streaming
    structured Streaming
    near real time data ingestion
    near real time machine learning
    
#SQL editor    
    sql server 
    
# easy collabaration
    git
    git hub   
    azure devops    
#ETL
    E(Spark Connector to Blob Storage or S3)
    T(Apache Spark)
    L(Hive Metastore)

workflows--->run the notebook job and sheduled orchistration


#databricks
    https://community.cloud.databricks.com/?o=1090844998533153#notebook/703828266784110/command/703828266784111

    workspace id: 1090844998533153

    Cluster: computational resource
        In the context of computers and information technology, a cluster refers to a group 
        of interconnected computers or servers that work together as a single system.
        
    Notebook: script

7.

    DROP TABLE IF EXISTS employeesData;
    CREATE TABLE employeesData USING CSV OPTIONS (path " /FSIeStore/tabIes/EmpIoyeesData.csv" , header "true")

8.  DBFS: it is abstraction on top of scalability object storage

    settings--->admin settings--->workspace settings--->Advanced--->DBFS File Browser(enable)

9.
    dbutils.fs.ls(/)
    display(dbutils.fs.ls(/))

    two ways to upload file
        1. data--->dbfs
        2. in notebook
    
10.

    dbutils utilities are available in python, scala, ReferenceError
    dbutils.help()
    dbutils.fs.help()
    dbutils.fs.help("cp")
    dbutils.fs.help("ls")

11.
    dbutils.help()
    dbutils.data.help()
    dbutils.data.help('summarize')
    data_student = [("Michael", "Science", 80,"P", 90),
                  ("Nancy", "Maths", 80,"P", None),
                  ("RG", "Science", 80, "F", 90),
                  ("Jathin", "English", None,"F", None),
                  ("Raj", "Hindi", None ,"P", 60),
                  ("prajay", "maths", None, None, 70)]
    Schema = ["name","mark","Status","Attendnce"]
    df= spark.createDataFrame(data=data_student, schema= Schema)
    display(df)

    dbuitils.data.summarize(df)
12.    
    
    commands: cp, head, ls, mkdir, mount, mounts, mv, put, refreshMounts,rm, unmount, updateMount.
                
    dbutils.fs.cp('file_path1',"file_path2")
    dbuitils.fs.head('file_path')
    dbuitils.fs.ls('/filestore)
    dbutils.fs.mv(file_path)
    dbutils.fs.put(file_path)
    
13. 
    dbutilis.help()
    dbutilis.fs.help()
    dbutilis.notebook.help()
    dbutilis.notebook.help('exit')
    
    firstname='Ganesh'
    dbutilis.notebook.exit(firstname)
    #below cells after exit() command will get skipped
    

14. #run()
    run() command of notebook utility(dbutils.notebook)
    dbutilis.notebook.help('run')
    
    firstname='ganesh'
    dbutilis.notebook.exit(firstname)
    
    var=dbutilis.notebook.run('notebook1',60)
    print(var)
    #run command is invoke one notebook inside another notebook

15. #Widgets utility
    It will allows you to parameterized in notebook.
    Commands: combobox, dropdown, get, getArgument, multiselect, remove, removeall, text
    
    dbuitils.widgets.help()
    
    dbuitils.widgets.combobox(name='fruitCB', defaultVaIue='apple', choices=['Ct apple', 'banana' , 'orange'] , label= 'Fruits ComboBox')
    #Combination of text and dropdown. Select a value from a provided list or input one in the text box
    dbutils.widgets.dropdown(name='fruitCB', defaultVaIue='apple', choices=['Ct apple', 'banana' , 'orange'] , label= 'Fruits dropdown')
    #Select a value from a list of provided values
    dbutils.widgets.multiselect(name='fruitCB', defaultVaIue='apple', choices=['Ct apple', 'banana' , 'orange'] , label= 'Fruits multiselect')
    # Select one or more values from a list of provided values.
    dbutils.widgets.text(name='fruitCB', defaultVaIue='apple', choices=['Ct apple', 'banana' , 'orange'] , label= 'Fruits multiselect')
    # Input a value in a text box
    
    # Create a widget with a default value
    dbutils.widgets.text("my_widget", "default_value")

    # Get the current value of the widget
    widget_value = dbutils.widgets.get("my_widget")
    print("Widget value:", widget_value)
    
    dbuitils.widgets.remove('fruitsCB')

    dbuitils.widgets.removeall()
    
 16.#Pass values to notebook parameters from another notebook using run command in Azure Databricks  
    dbutils.widgets.combobox (name= 'Paramdropdown', defaultVaIue='apple', choices=['apple', 'banana','orange'],label='Fruits ComboBox')
    dbutils.widgets.dropdown (name= 'Paramdropdown', defaultVaIue='apple', choices=['apple', 'banana','orange'],label='Fruits dropdown')
    dbutils.widgets.multiselect (name= 'Paramdropdown', defaultVaIue='apple', choices=['apple', 'banana','orange'],label='Fruits Multiselect')
    dbutils.widgets.text (name= 'Paramtext', defaultVaIue='apple', label='Fruits ComboBox')
    
    print( 'combobox value is' + dbutils.widgets.get('Paramcombobox') )
    print( 'dropdown value is' + dbutits.widgets.get('Paramdropdown'))
    print( 'multi select value is' + dbutils.widgets.get('Parammultlselect'))
    print( 'text value is' + dbutils.widgets.get( ) )
    
    dbuitils.notebook.run('/Users/mail_id/demonotebook',120,{'Paramcombobox':'ComboBox','Paramdropdown':'dropdown','Parammultlselect':'multiselect':'Paramtext':'text'})

17. #Parameterize SQL notebook using widgets in Azure Databricks 

    #create Widgets in SQL Parameterize notebook in Azure Databricks
    
    #Create cluster
    %sql
        CREATE WIDGET TEXT y DEFAULT "10"
        CREATE WIDGET DROPDOWN cuts DEFAULT "Good" CHOICES SELECT DISTINCT cut FROM diamonds
        SELECT * FROM persons where gender = getArgument('gender')
        SELECT * FROM persons where gender = '$gender'    
        
        remove widget firstname        
        
        remove widget firstname 
    %python
        data=[(1,'ganesh','male'), (2, 'pradeep' , 'male') , (3,'jathin','male'),(4,'radha','female')]
        cols =['name', ' gender')
        df= spark.(data,colscreateDataFrame)
        display (df)
    %python
        df.createOrReolaceTemView('persons')
        select * from persons
        
        create widget Dropdown genderDD DEFAULT 'male' choice select gender fro persons
    
        remove widget genderDD
        
        select * from persons where gender = getArgument('gender')
        
18. #Create Mount point using dbutils.fs.mount() in Azure Databricks        
    dbutils.fs.help()
    
    dbutils.fs.mount(
      source: str,
      mount_point: str,
      encryption_type: Optional[str] = "",
      extra_configs: Optional[dict[str:str]] = None)
    
    dbutils.fs.ls('/mnt/blobstorage')
    
    dbutils.fs.cp('/mnt/blobstorage/file_name.csv')
    
19. #Mount Azure Blob Storage to DBFS in Azure Databricks
    dbutils.fs.mount(
      source = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net",
      mount_point = "/mnt/iotdata",
      extra_configs = {"fs.azure.account.key.<storage-account-name>.blob.core.windows.net":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})
      
      #two ways
          1. Account key
          2. SAS key
     dbutils.fs.help()
     
     #keys
     goto--->storage--->access-key--->copy
     dbutils.fs.ls('/mnt/blobstorage1')
     dbutils.fs.cp('/mnt/blobstorage1/input/demo.json')
     
20. #Delete or Unmount Mount Points in Azure Databricks    
    dbutils.fs.unmount("/mnt/<mount-name>")
    df = ('mnt/blobstorage1/EmpIoyees.csv',header = True)
    df.show()
    
    dbutils.fs.help()
    
21. #mounts() & refreshMounts() commands of File system Utilities in Azure Databricks
 
    mounts command (dbutils.fs.mounts) Displays information about
    what is currently mounted within DBFS.
    
    refreshMounts command (dbutils.fs.refreshMounts) Forces all
    machines in the cluster to refresh their mount cache, ensuring they
    receive the most recent information.
    
    dbutils.fs.help()
    
    dbutils.fs.mounts()
    
    dbutils.fs.refreshMounts()
    
22. #Update Mount Point(dbutils.fs.updateMount()) in Azure Databricks
    
    updateMount()
    Similar to the dbutils.fs.mount command, but updates an existing
    mount point instead of creating a new one. Returns an error if the
    mount point is not present.
    
    dbutils.fs.updateMount(
  source = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net",
  mount_point = "/mnt/<mount-name>",
  extra_configs = {"<conf-key>":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})
  
23. #Secret Scopes Overview in Azure Databricks
    Secrets Management
        Managing secrets begins with creating a secret scope. A secret scope is
        collection of secrets identified by a name. A workspace is limited to a
        maximum of 100 secret scopes
        
        There are two types of secret scopes.
            Azure Key Vault-backed scopes
            Databricks-backed scopes
       dbutils.fs.mount(
      source = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net",
      mount_point = "/mnt/iotdata",
      extra_configs = {"fs.azure.account.key.<storage-account-name>.blob.core.windows.net":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})
      
    dbuitils.secrets.help()

24. #Install Databricks CLI and configure with your workspace | Azure Databricks
    #install databricks in your local system 
    install databricks-cli
    databricks --help
    #connect with cmd to databricks
    databricks configure --help
    databricks configure --token
    databricks Host (should begin with https://): https://adb-741642928Ø338325.5.azuredatabricks.net/
    Token:(paste token from user setting in databricks
    databricks fs --help
    databricks fs ls
    databricks fs ls dbfs:/FileStore/temp1/

25. #Create an Azure Key Vault backed secret scope using the UI in Azure Databricks
    Go to https://<databricks-instance> #secrets/createScope
    Use the Manage Principal drop-down to specify whether All Users have
    MANAGE permission for this secret scope or only the Creator of the
    secret scope.
    Your account must have the Premium Plan for you to be able to select
    Creator.
    Use the Databricks CLI databricks secrets list-scopes command to verify
    that the scope was created successfully.
    #local cmd
    databricks secrets list-scopes
    #data notebook
    dbuitils.secrets.get('testScope','blobstorage')
    dbuitils.secrets.help()
    
26. #Create a Databricks backed secret scope in Azure Databricks
    #A Databricks-backed secret scope is stored in (backed by) an encrypted
    #database owned and managed by Azure Databricks.
    databricks secrets create-scope --scope <scope-name> --initial-manage-
    principal users
    
    databricks secrets put --scope <scope-name> --key <key-name>
    
    databricks secrets list --scope <scope-name>
    
    databricks secrets delete-scope --scope <scope-name>
    #local cmd
    databricks --help
    databricks secrets list-scopes
    databricks secrets --help
    databricks secrets create-scope --help
    databricks secrets create-scope --scope demoScope
    
    databricks secrets list-scope
    
    databricks secrets put --scope demoScope --key myBlob
    
    databricks secrets list-scope demoscope
 
 27. #Secrets Utility(dbutils.secrets) of Databricks Utilities in Azure Databricks
    dbutils.secrets
    The secrets utility allows you to store and access sensitive credential
    information withoutmaking-them visibe in notebooks.
    
    commands: get, getBytes, list, listScopes
    
    databricks secrets list-scopes
    
    databricks secrets list --scope demoScope
    
    #in Databricks
    dbutils.secret.help()
    
    dbutils.secrets.get('demoScope','myBlob')
    
    dbutils.secrets.getBytes('demoScope','myBlob')
    
    dbutils.secrets.getBytes('demoScope','myBlob').decode('utf-8')
    
    dbutils.secrets.listScopes()
    
28. #Access ADLS Gen2 storage using Account Key in Azure Databricks
    Steps to Access ADLS Gen2
        • Create secret scope and secret
        • Set configuration for spark session using account Key
        • Access Storage
    spark.conf.set(
    "fs.azure.account.key.<storage-account-name>.dfs.core.windows.net",
    dbutils.secrets.get(scope="<scope-name>",key="<storage-account-access-key-name>"))
    
    df =spark.read.csv(path='abfss://samplecontainer@adlsganesh.dfs.core.windows.net/data/Employees.csv', header=True)
    display(df)
29. #Configure access to Azure storage with an Azure Active Directory service principal
    • Configure access to Azure storage with an Azure service principal
    • Create an service_principle
    • Assign access to it on Azure storage Active Directory
    
30.#30.Access Data Lake Storage Gen2 or Blob Storage with an Azure service principal in Azure Databricks
    Access ADLS Gen2 with Service Principle
    service_credential = dbutils.secrets.get(scope="<scope>",key="<service-credential-key>")
    spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
    spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net",
    "<application-ld>")
    spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net",
    service_credential)
    spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net",
    "https://login.microsoftonline.com/<directory-id>/oauth2/token")
31. #Access ADLS Gen2 or Blob Storage using a SAS token in Azure Databricks
    Access ADLS Gen2 with SAS Token
    spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "SAS")
    spark.conf.set("fs.azure.sas.token.provider.type.<storage-account>.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    spark.conf.set("fs.azure.sas.fixed.token.<storage-account>.dfs.core.windows.net","<token>"