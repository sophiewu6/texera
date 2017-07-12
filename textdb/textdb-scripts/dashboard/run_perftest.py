textdb_workspace = "/home/jamshid/workspace/"
maven_repo_home = "/home/jamshid/.m2/repository/"
java8_bin = "/usr/bin/java"

textdb_home = "textdb/textdb/"
result_path = "textdb-perftest/perftest-files/results/"
branch = "master"
main_class = "edu.uci.ics.textdb.perftest.sample.MedlineExtraction"
# Refer to the codebase to understand what arguments the main class takes in.
perftest_arguments = ["/home/bot/textdbworkspace/data-files/", "\"\"","\"\"","\"\"","\"\""]


textdb_path = textdb_workspace + textdb_home
textdb_perftest_path = textdb_path + "textdb-perftest/"
result_folder = textdb_workspace + textdb_home + result_path

print "" + \
        java8_bin + " " + \
        "-Dfile.encoding=UTF-8 -classpath" + " " + \
        textdb_workspace + "/textdb/textdb/textdb-perftest/target/classes" + ":" + \
        textdb_workspace + "/textdb/textdb/textdb-api/target/classes" + ":" + \
        textdb_workspace + "/textdb/textdb/textdb-exp/target/classes" + ":" + \
        textdb_workspace + "/textdb/textdb/textdb-storage/target/classes" + ":" + \
        maven_repo_home + \
        maven_repo_home + "junit/junit/4.8.1/junit-4.8.1.jar" + ":" + \
        maven_repo_home + "org/apache/lucene/lucene-core/5.5.0/lucene-core-5.5.0.jar" + ":" + \
        maven_repo_home + "org/apache/lucene/lucene-analyzers-common/5.5.0/lucene-analyzers-common-5.5.0.jar" + ":" + \
        maven_repo_home + "org/apache/lucene/lucene-queryparser/5.5.0/lucene-queryparser-5.5.0.jar" + ":" + \
        maven_repo_home + "org/apache/lucene/lucene-queries/5.5.0/lucene-queries-5.5.0.jar" + ":" + \
        maven_repo_home + "org/apache/lucene/lucene-sandbox/5.5.0/lucene-sandbox-5.5.0.jar" + ":" + \
        maven_repo_home + "org/json/json/20160212/json-20160212.jar" + ":" + \
        textdb_workspace + "/textdb/textdb/textdb-api/target/classes" + ":" + \
        textdb_workspace + "/textdb/textdb/textdb-common/target/classes" + ":" + \
        textdb_workspace + "/textdb/textdb/textdb-dataflow/target/classes" + ":" + \
        maven_repo_home + "com/google/re2j/re2j/1.1/re2j-1.1.jar" + ":" + \
        maven_repo_home + "com/fasterxml/jackson/core/jackson-databind/2.5.3/jackson-databind-2.5.3.jar" + ":" + \
        maven_repo_home + "com/fasterxml/jackson/core/jackson-core/2.5.3/jackson-core-2.5.3.jar" + ":" + \
        maven_repo_home + "com/fasterxml/jackson/core/jackson-annotations/2.5.3/jackson-annotations-2.5.3.jar" + ":" + \
        textdb_workspace + "/textdb/textdb/textdb-storage/target/classes" + ":" + \
        maven_repo_home + "edu/stanford/nlp/stanford-corenlp/3.6.0/stanford-corenlp-3.6.0.jar" + ":" + \
        maven_repo_home + "com/io7m/xom/xom/1.2.10/xom-1.2.10.jar" + ":" + \
        maven_repo_home + "xml-apis/xml-apis/1.3.03/xml-apis-1.3.03.jar" + ":" + \
        maven_repo_home + "xerces/xercesImpl/2.8.0/xercesImpl-2.8.0.jar" + ":" + \
        maven_repo_home + "xalan/xalan/2.7.0/xalan-2.7.0.jar" + ":" + \
        maven_repo_home + "joda-time/joda-time/2.9/joda-time-2.9.jar" + ":" + \
        maven_repo_home + "de/jollyday/jollyday/0.4.7/jollyday-0.4.7.jar" + ":" + \
        maven_repo_home + "javax/xml/bind/jaxb-api/2.2.7/jaxb-api-2.2.7.jar" + ":" + \
        maven_repo_home + "com/googlecode/efficient-java-matrix-library/ejml/0.23/ejml-0.23.jar" + ":" + \
        maven_repo_home + "javax/json/javax.json-api/1.0/javax.json-api-1.0.jar" + ":" + \
        maven_repo_home + "org/slf4j/slf4j-api/1.7.12/slf4j-api-1.7.12.jar" + ":" + \
        maven_repo_home + "edu/stanford/nlp/stanford-corenlp/3.6.0/stanford-corenlp-3.6.0-models.jar" + ":" + \
        maven_repo_home + "org/mockito/mockito-all/1.9.5/mockito-all-1.9.5.jar" + " " + \
        main_class
