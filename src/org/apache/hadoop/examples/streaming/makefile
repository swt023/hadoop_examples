CC = g++
CPPFLAGS = -m64 -I/usr/local/hadoop/include/

wc: WordCount.cpp
	$(CC) $(CPPFLAGS) $< -Wall -L/usr/local/hadoop/lib/native/ -lhadooppipes -lhadooputils -lpthread  -L/usr/lib/vmware-tools/lib64/ -lcrypto -g -O2 -o $@
