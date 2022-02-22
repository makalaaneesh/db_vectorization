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
        printf("]");
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
            sample_tuple->len = 2;
            sample_tuple->integers = (int *) malloc(sizeof(int) * 2);
//            sample_tuple->print();
            for(size_t i = 0; i < 2; i++){
                iss >> sample_tuple->integers[i];
            }
//            sample_tuple->print();

            return sample_tuple;
        } else{
            return NULL;
        }


//        Tuple *sample_tuple = (struct Tuple*) malloc(sizeof(struct Tuple));
//        sample_tuple->integers = (int *) malloc(sizeof(int) * 2);
//        sample_tuple->integers[0] = 1; sample_tuple->integers[1] = 2;
//        sample_tuple->len = 2;
//        return sample_tuple;
    }
};


class ArithmeticOperationExecutor: public Executor {
        public:
        string op;
        size_t column1index, column2index;
        Executor* childExecutor;

        ArithmeticOperationExecutor(string _op, size_t _column1index, size_t _column2index, Executor* _childExecutor){
            op = _op;
            column1index = _column1index;
            column2index = _column2index;
            childExecutor = _childExecutor;
        }

        struct Tuple* next(){
            Tuple* result_tuple = (struct Tuple*) malloc(sizeof(struct Tuple));
            result_tuple->integers = (int *) malloc(sizeof(int) * 1);
            result_tuple->len = 1;

            Tuple* input_tuple = childExecutor->next();
            if (input_tuple == NULL){
                return input_tuple;
            }
//            input_tuple->print();
            result_tuple->integers[0] = input_tuple->integers[0] + input_tuple->integers[1];
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
//            input_tuple->print();
            for(size_t i=0; i<numColumns; i++){
                result_tuple->integers[i] = input_tuple->integers[columnIndices[i]];
            }
            return result_tuple;
        }
};

int main(){
    Tuple* final_result;
    // SELECT a from table;
//    SequentialScanExecutor sse("sample_table");
//    size_t columns[] = {0};
//    SelectionExecutor se(columns, 1, &sse);
//    while (true){
//        final_result = se.next();
//        if (final_result == NULL) break;
//        final_result->print();
//    }



    // SELECT a+b from table;
    SequentialScanExecutor sse("sample_table");
    ArithmeticOperationExecutor aoe("+", 0, 1, &sse);
    while (true){
        final_result = aoe.next();
        if (final_result == NULL) break;
        final_result->print();
    }

}