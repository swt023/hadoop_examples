#include <string>
#include <map>
#include <iostream>
#include <string.h>
#include <stdlib.h>

#include "Pipes.hh"
#include "TemplateFactory.hh"
#include "StringUtils.hh"

using namespace std;

class WordCountMapper : public HadoopPipes::Mapper{
public:
	WordCountMapper(HadoopPipes::TaskContext &context){}
	void map(HadoopPipes::MapContext &context){
		char *sep = "!,.; ";
		char *p = NULL;
		string line = context.getInputValue();
		char *buf = (char *)malloc(line.length() + 1);
		strcpy(buf, line.c_str());
		p = strtok(buf, sep);
		while ( p != NULL) {
			cout<<"wdy word: "<<p<<endl;
			context.emit(p, HadoopUtils::toString(1));
			p = strtok(NULL, sep);
		}
	}

};

class WordCountReducer : public HadoopPipes::Reducer{
public:
	WordCountReducer(HadoopPipes::TaskContext &context){}
	void reduce(HadoopPipes::ReduceContext &context) {
		string word = context.getInputKey();
		int sum = 0;
		int val = 0;
		while (context.nextValue()) {
			val = HadoopUtils::toInt(context.getInputValue());
			sum += val;
			cout<<"wdy val: "<<val<<endl;
		}
		context.emit(word, string(HadoopUtils::toString(sum)));
	}

};


int main(int argc, char **argv) {
	return HadoopPipes::runTask(HadoopPipes::TemplateFactory<WordCountMapper, WordCountReducer>());
}
