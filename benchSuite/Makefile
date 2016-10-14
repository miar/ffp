#Tested with JDK 1.8.0_66
# make sure that JDK and JDK_PATH are OK. 
# I assuming that you have it in  ~/.bin. Please change it otherwise

JDK_PATH = ~/.bin
JDK = jdk1.8.0_66/bin

TEST = MultiThreadBenchRunner

MultiThreadBenchRunnerAll:
	make GetUpdatedFFP
	$(JDK_PATH)/$(JDK)/javac *.java
#       $(JDK_PATH)/$(JDK)/java  -server $(TEST) generateNewDataset  CHM     CSLM    NBHM     CT     FFP    > tmp/map_results
	$(JDK_PATH)/$(JDK)/java  -server $(TEST)      "true"        "true"  "false" "false" "false" "false" > tmp/CHM
	$(JDK_PATH)/$(JDK)/java  -server $(TEST)      "false"       "false" "true"  "false" "false" "false" > tmp/CSLM
	$(JDK_PATH)/$(JDK)/java  -server $(TEST)      "false"       "false" "false" "true"  "false" "false" > tmp/NBHM
	$(JDK_PATH)/$(JDK)/java  -server $(TEST)      "false"       "false" "false" "false" "true"  "false" > tmp/CT
	$(JDK_PATH)/$(JDK)/java  -server $(TEST)      "false"       "false" "false" "false" "false" "true"  > tmp/FFP
	make clean

GetUpdatedFFP:
	cp ../ffp/src/* ffp/
	cd ffp ; $(JDK_PATH)/$(JDK)/javac -XDignore.symbol.file *.java	

clean: 
	rm *.class

