# hadooptemplate
hadoop template for test run hadoop 2.6.0

# Running on Windows 10/11
- Download Java JDK 1.8 or equivalent
- Download Hadoop 2.6.0 (tar.gz, not the source!) https://archive.apache.org/dist/hadoop/core/hadoop-2.6.0/
- Unzip all to a location, preferably without spaces (due to hadoop weird issues)
- Edit Environment Variables > User Variables
- HADOOP_HOME = (example) F:\hadoop-2.6.0
- JAVA_HOME = (example) F:\Java\jdk1.8.0_144

![environment](https://i.imgur.com/xRJNpFM.png)

- Add %HADOOP_HOME%\bin to your PATH as well
- Clone this https://github.com/steveloughran/winutils
- Unzip and put everything from the cloned hadoop-2.6.0/bin into your own hadoop folder (example) F:\hadoop-2.6.0\bin
- Clone this project
- Run with IntelliJ, with program arguments "input output" without the quotation marks
- You should find an output folder with the word count

# Building the JAR file to run in Hadoop FS
- File > Project Structure > Artifacts > + new JAR
- Select from module with dependencies
- Select your Main class file
- Select Extract to target JAR
- Directory for META-INF change to src/main/resources instead of src/main/java
- Apply, and OK to close
- Build > Build Artifacts > Build/Rebuild
- Run the jar file via "hadoop jar 'pathtoyourjar.jar'
- **if there is an issue regarding license, open the jar file and delete META-INF/license folder**
