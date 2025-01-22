//
// Created by liyu on 12/23/23.
//

#include "vector/TimestampColumnVector.h"

TimestampColumnVector::TimestampColumnVector(int precision, bool encoding): ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    TimestampColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, encoding);
}

TimestampColumnVector::TimestampColumnVector(uint64_t len, int precision, bool encoding): ColumnVector(len, encoding) {
    std::cout<<precision<<std::endl;
    this->precision = precision;
    if(encoding) {
        posix_memalign(reinterpret_cast<void **>(&this->times), 64,
                       len * sizeof(long));
    } else {
        posix_memalign(reinterpret_cast<void **>(&this->times), 64,
                       len * sizeof(long));
    }
    memoryUsage += (long) sizeof(long) * len;
}


void TimestampColumnVector::close() {
    if(!closed) {
        ColumnVector::close();
        if(encoding && this->times != nullptr) {
            free(this->times);
        }
        this->times = nullptr;
    }
}

void TimestampColumnVector::print(int rowCount) {
    throw InvalidArgumentException("not support print longcolumnvector.");
//    for(int i = 0; i < rowCount; i++) {
//        std::cout<<longVector[i]<<std::endl;
//		std::cout<<intVector[i]<<std::endl;
//    }
}

TimestampColumnVector::~TimestampColumnVector() {
    if(!closed) {
        TimestampColumnVector::close();
    }
}

void * TimestampColumnVector::current() {
    if(this->times == nullptr) {
        return nullptr;
    } else {
        return this->times + readIndex;
    }
}

/**
     * Set a row from a value, which is the days from 1970-1-1 UTC.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param days
 */
void TimestampColumnVector::set(int elementNum, long ts) {
    if(elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }
    times[elementNum] = ts;
    // TODO: isNull
    isNull[elementNum] = NULL;
}

long TimestampColumnVector::stringTimestampToSeconds(const std::string& value) {
    struct std::tm tm {};
    if (strptime(value.c_str(), "%Y-%m-%d %H:%M:%S", &tm)) {
        std::time_t time = std::mktime(&tm);
        
        long timestamp = static_cast<long>(time) + 8 *3600;

        if(precision == 0)
            precision = 6;

        if (precision == 3) { // 毫秒
            timestamp *= 1000;
        } else if (precision == 6) { // 微秒
            timestamp *= 1000000;
        }

        return timestamp;
    } else {
        std::cerr << "Failed to parse date string: " << value << std::endl;
    }
}

void TimestampColumnVector::ensureSize(uint64_t size, bool preserveData) {
    ColumnVector::ensureSize(size, preserveData);
    if (length < size) {
		long *oldVector = times;
        posix_memalign(reinterpret_cast<void **>(&times), 32,
                        size * sizeof(int64_t));
        if (preserveData) {
            std::copy(oldVector, oldVector + length, times);
        }
        delete[] oldVector;
        memoryUsage += (long) sizeof(long) * (size - length);
        resize(size);
    }
}

void TimestampColumnVector::add(std::string &value) {
    if(writeIndex >= length){
        ensureSize(writeIndex * 2, true);
    }
   std::cout<<"stringTimestampToSeconds"<<stringTimestampToSeconds(value)<<std::endl;
   set(writeIndex++,stringTimestampToSeconds(value));//这里不对
}

void TimestampColumnVector::add(int64_t value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);  
    }
    int index = writeIndex++;
    times[index] = value;
    isNull[index] = false;    
}