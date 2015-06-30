export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar

mkdir dataset
wget -c http://www.gutenberg.org/ebooks/2264.txt.utf-8 -P dataset
wget -c http://www.gutenberg.org/ebooks/2265.txt.utf-8 -P dataset
wget -c http://www.gutenberg.org/ebooks/2267.txt.utf-8 -P dataset
wget -c http://www.gutenberg.org/ebooks/1513.txt.utf-8 -P dataset

hadoop fs -mkdir -p /tutorial/input
hadoop fs -put ./dataset/* /tutorial/input

echo
echo
echo "=============== 1st =============="
cd ./first_example
mkdir build
hadoop com.sun.tools.javac.Main WordCount.java -d build
jar -cvf WordCount.jar -C ./build ./
hadoop jar WordCount.jar WordCount /tutorial/input /tutorial/output
hadoop fs -cat /tutorial/output/*
hadoop fs -rm -r -f /tutorial/output/
cd -

echo
echo
echo "=============== 2nd =============="
cd ./second_example
mkdir build
hadoop com.sun.tools.javac.Main WordCount.java -d build
jar -cvf WordCount.jar -C ./build ./
hadoop jar WordCount.jar WordCount /tutorial/input /tutorial/output
hadoop fs -cat /tutorial/output/*
hadoop fs -rm -r -f /tutorial/output/
cd -

echo
echo
echo "=============== 3rd =============="
cd ./third_example
mkdir build
hadoop com.sun.tools.javac.Main TopWords.java -d build
jar -cvf TopWords.jar -C ./build ./
hadoop jar TopWords.jar TopWords /tutorial/input /tutorial/output
hadoop fs -cat /tutorial/output/*
hadoop fs -rm -r -f /tutorial/output/
cd -


