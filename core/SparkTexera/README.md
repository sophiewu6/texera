How to run the Texera Spark implementation for a single machine?

1. Clone the Texera Github

2. Open the texera/core/SparkTexera folder with IntelliJ

3. Go to File -> Project Structure. Click "Project" tab on the left under the "Project Settings" layer. Make sure the "Project SDK" is set to java 1.8

4. Click "Module" tab under the "Project Settings" layer. In the "Sources" tab, click the folder scr/main/java and click "Sources"(the blue one) in the "Mark as:" to make it the Source Folders of the IntelliJ. You should see it been added in the right of the Project structure window

5. Run the main function under the src/main/java/Driver/TexeraWebApplication file. This will generate a build configuration and the program will exit immediately. Edit the build configuration by clicking the drop down menu beside the run button(the green arrow) in the right top corner. Select "Edit configurations", under the configuration tab on the right, inside "Program arguments", add "server config.yaml".

6. Run the TexeraWebApplication file again and the backend should be able to run, you should see the server waiting for request in the terminal

7. Start the frontend by going to texera/core/new-gui, run "npm install" then "ng serve".

8. Go to the texera application website localhost:4200.

9. Drag 3 operators to form a workflow(view result is the current temp count operator). In side filescan operator put the test data file path in the "File Path" attribute. Write something in the "Result Attribute" since its required for the front end

10. Inside keyword search operator, put the keyword query in the "Query" attribute. In the "Attribute", write the column name as "_c*"(such as _c0 for first column and _c2 for third column). Fill in the "Macthing type" and "Span List name" attribute.

11. Inside view result operator, make sure the number of "limit" attribute is greater or equal to the number of rows of the data file.

12. Click "Run" to run the Spark application. You can go to localhost:4040 to check the execution stastics(including running time) of the Spark application. This service will be closed if you stop the backend server.

How to run the Java Implementation of the workflow

Both single thread and multi-thread implementation are stored under the src/main/java folder. Just run the corresponding main function to run the implementation. To change the input file, change the path for the "file" variable.

Data file Google Drive Link:
https://drive.google.com/open?id=1CIi77XqhDmxUGHRxm1MLAzrtfm6vvZq3