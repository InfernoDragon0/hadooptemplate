# hadooptemplate
hadoop template for test run hadoop 2.6.0

# Running on Windows 10/11
- Download Java JDK 1.8 or equivalent
- Download Hadoop 2.6.0 (tar.gz, not the source!) https://archive.apache.org/dist/hadoop/core/hadoop-2.6.0/
- Unzip all to a location, preferably without spaces (due to hadoop weird issues)
- Edit Environment Variables > User Variables
- HADOOP_HOME = (example) F:\hadoop-2.6.0
- JAVA_HOME = (example) F:\Java\jdk1.8.0_144
- Clone this https://github.com/steveloughran/winutils
- Unzip and put everything from the cloned hadoop-2.6.0/bin into your own hadoop folder (example) F:\hadoop-2.6.0\bin
- Clone this project
- Run with IntelliJ, with program arguments "input output" without the quotation marks
- You should find an output folder with the word count
