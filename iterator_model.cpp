#include<iostream>
#include <fstream>
#include <sstream>

using namespace std;
// Dealing with only ints
// try just select itself should be faster in vector format. less trips to disk
// try a + b
// try a == b

class Tuple{
public:
    int *integers;
    size_t len;

    void print(){
        printf("[");
        for(size_t i=0; i<len; i++){
            printf("%d,", integers[i]);
        }
        printf("]\n");
    }
};

class Executor{
public:
    virtual struct Tuple* next() = 0;
};

class SequentialScanExecutor: public Executor  {
public:
    string filename;
    ifstream fs;

    SequentialScanExecutor(string _filename){
        filename = _filename;
        fs.open(filename);

    }

//        https://stackoverflow.com/questions/14600489/reading-data-file-into-2d-array-c
    struct Tuple* next(){
        string line;
        getline(fs, line);
        if (fs){

            istringstream iss(line);
            Tuple *sample_tuple = (struct Tuple*) malloc(sizeof(struct Tuple));
            sample_tuple->len = 1;
            sample_tuple->integers = (int *) malloc(sizeof(int) * 1);
//            sample_tuple->print();
            for(size_t i = 0; i < 1; i++){
                iss >> sample_tuple->integers[i];
            }

            return sample_tuple;
        } else{
            return NULL;
        }
    }
};

//class SequentialScanMemoryExecutor: public Executor  {
//public:
//    string filename;
//    int* table;
//    size_t i;
//    size_t len;
//
//    SequentialScanMemoryExecutor(int *_table, size_t _len){
//        table = _table;
//        len = _len;
//        i = 0;
//    }
//
//    struct Tuple* next(){
//        Tuple *sample_tuple = (struct Tuple*) malloc(sizeof(struct Tuple));
//        sample_tuple->len = 2;
//        sample_tuple->integers = (int *) malloc(sizeof(int) * 2);
//        if (i >=len){
//            return NULL;
//        }
//        sample_tuple->integers[0] = table[i++];
//        sample_tuple->integers[1] = table[i++];
//
//        return sample_tuple;
//    }
//};

class AggregationOperationExecutor: public Executor {
public:
    string op;
    size_t columnindex;
    Executor* childExecutor;
    size_t aggValue;
    size_t len;
    bool computed;

    AggregationOperationExecutor(string _op, size_t _columnindex, Executor* _childExecutor){
        op = _op;
        columnindex = _columnindex;
        childExecutor = _childExecutor;
        aggValue = 0;
        computed = 0;
        len = 0;
    }

    struct Tuple* next(){
        if (computed){
            return NULL;
        }
        Tuple* result_tuple = (struct Tuple*) malloc(sizeof(struct Tuple));
        result_tuple->integers = (int *) malloc(sizeof(int) * 1);
        result_tuple->len = 1;

        while (true) {
            Tuple* input_tuple = childExecutor->next();
            if (input_tuple == NULL){
                break;
            }
            aggValue += input_tuple->integers[columnindex];
            len++;
        }
        aggValue = aggValue/len;

//            input_tuple->print();
        result_tuple->integers[0] = aggValue;
        computed = 1;
        return result_tuple;
    }
};

class SelectionExecutor: public Executor {
        public:
        size_t* columnIndices;
        size_t numColumns;
        Executor* childExecutor;

        SelectionExecutor(size_t* _columnIndices, size_t _numColumns, Executor* _childExecutor){
            columnIndices = _columnIndices;
            numColumns = _numColumns;
            childExecutor = _childExecutor;
        }

        struct Tuple* next(){
            Tuple* result_tuple = (struct Tuple*) malloc(sizeof(struct Tuple));
            result_tuple->integers = (int *) malloc(sizeof(int) * numColumns);
            result_tuple->len = numColumns;

            Tuple* input_tuple = childExecutor->next();
            if (input_tuple == NULL){
                return input_tuple;
            }
            for(size_t i=0; i<numColumns; i++){
                result_tuple->integers[i] = input_tuple->integers[columnIndices[i]];
            }
            return result_tuple;
        }
};

int main(){
//    size_t len = 100000000;
//    int * table = (int *) malloc(sizeof(int) * len);
//    for (int i = 0; i < len; ++i) {
//        table[i] = i;
//    }

    Tuple* final_result;

//    // SELECT avg(a) from table;
//    SequentialScanMemoryExecutor sse(table, len);
    SequentialScanExecutor sse("sample_table");
    AggregationOperationExecutor aoe("+", 0, &sse);
    while (true){
        final_result = aoe.next();
        if (final_result == NULL) break;
        final_result->print();
    }


}