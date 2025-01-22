//
// Created by yuly on 06.04.23.
//

#include "vector/DateColumnVector.h"

DateColumnVector::DateColumnVector(uint64_t len, bool encoding): ColumnVector(len, encoding) {
	std::cout<<encoding<<std::endl;
    if(encoding) {
        posix_memalign(reinterpret_cast<void **>(&dates), 32,
                       len * sizeof(int32_t));
	} else {
		posix_memalign(reinterpret_cast<void **>(&dates), 32,
                       len * sizeof(int32_t));
	}
	memoryUsage += (long) sizeof(int) * len;
}

void DateColumnVector::close() {
	if(!closed) {
		if(encoding && dates != nullptr) {
			free(dates);
		}
		dates = nullptr;
		ColumnVector::close();
	}
}

void DateColumnVector::print(int rowCount) {
	for(int i = 0; i < rowCount; i++) {
		std::cout<<dates[i]<<std::endl;
	}
}

DateColumnVector::~DateColumnVector() {
	if(!closed) {
		DateColumnVector::close();
	}
}

/**
     * Set a row from a value, which is the days from 1970-1-1 UTC.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param days
 */
void DateColumnVector::set(int elementNum, int days) {
	if(elementNum >= writeIndex) {
		writeIndex = elementNum + 1;
	}
	dates[elementNum] = days;
	// TODO: isNull
	isNull[elementNum] = false;
}

void * DateColumnVector::current() {
    if(dates == nullptr) {
        return nullptr;
    } else {
        return dates + readIndex;
    }
}

// 判断是否为闰年
bool isLeapYear(int year) {
    return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
}

// 每个月的天数
int daysInMonth(int year, int month) {
    switch (month) {
        case 1: case 3: case 5: case 7: case 8: case 10: case 12:
            return 31;
        case 4: case 6: case 9: case 11:
            return 30;
        case 2:
            return isLeapYear(year) ? 29 : 28;
        default:
            return 0; // 无效的月份
    }
}

int getDaysFromEpoch(const std::string& date_str) {
    int year, month, day;
    if (sscanf(date_str.c_str(), "%d-%d-%d", &year, &month, &day) != 3) {
        std::cerr << "Invalid date format" << std::endl;
        return -1;
    }

    // 计算从1970年到目标日期的天数
    int days = 0;
    
    // 计算从1970年到目标年份的天数
    for (int y = 1970; y < year; ++y) {
        days += isLeapYear(y) ? 366 : 365;
    }

    // 计算目标年份前几个月的天数
    for (int m = 1; m < month; ++m) {
        days += daysInMonth(year, m);
    }

    // 加上目标日期所在月的天数
    days += day - 1;  // 因为1970-01-01算作第0天，所以减1

    return days;
}

void DateColumnVector::ensureSize(uint64_t size, bool preserveData) {
    ColumnVector::ensureSize(size, preserveData);
    if (length < size) {
		int *oldVector = dates;
		posix_memalign(reinterpret_cast<void **>(&dates), 32,
						size * sizeof(int32_t));
		if (preserveData) {
			std::copy(oldVector, oldVector + length, dates);
		}
		delete[] oldVector;
		memoryUsage += (long) sizeof(int) * (size - length);
		resize(size);
    }
}

void DateColumnVector::add(int value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    int index = writeIndex++;
    // if(isLong) {
    //     longVector[index] = value;
    // } else {
    //     intVector[index] = value;
    // }
	dates[index-1] = value;//???为什么这里不是index而是index-1
    isNull[index] = false;
}

// void DateColumnVector::add(int64_t value) {
//     if (writeIndex >= length) {
//         ensureSize(writeIndex * 2, true);
//     }
//     int index = writeIndex++;
//     // if(isLong) {
//     //     longVector[index] = value;
//     // } else {
//     //     intVector[index] = value;
//     // }
//     isNull[index] = false;
// }

void DateColumnVector::add(std::string &value) {
    if(writeIndex >= length){
        ensureSize(writeIndex * 2, true);
    }
   std::cout<<getDaysFromEpoch(value)<<std::endl;
   set(writeIndex++,getDaysFromEpoch(value));//这里不对
   
}