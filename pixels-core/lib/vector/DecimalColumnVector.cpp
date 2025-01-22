//
// Created by yuly on 05.04.23.
//

#include "vector/DecimalColumnVector.h"
#include "duckdb/common/types/decimal.hpp"

/**
 * The decimal column vector with precision and scale.
 * The values of this column vector are the unscaled integer value
 * of the decimal. For example, the unscaled value of 3.14, which is
 * of the type decimal(3,2), is 314. While the precision and scale
 * of this decimal are 3 and 2, respectively.
 *
 * <p><b>Note: it only supports short decimals with max precision
 * and scale 18.</b></p>
 *
 * Created at: 05/03/2022
 * Author: hank
 */

DecimalColumnVector::DecimalColumnVector(int precision, int scale, bool encoding): ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    DecimalColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, scale, encoding);
}

DecimalColumnVector::DecimalColumnVector(uint64_t len, int precision, int scale,
                                         bool encoding)
    : ColumnVector(len, encoding) {
    // decimal column vector has no encoding so we don't allocate memory to
    // this->vector
    this->vector = nullptr;
    this->precision = precision;
    this->scale = scale;

    using duckdb::Decimal;
    if (precision <= Decimal::MAX_WIDTH_INT16) {
        physical_type_ = PhysicalType::INT16;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       len * sizeof(int16_t));
        memoryUsage += (uint64_t)sizeof(int16_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT32) {
        physical_type_ = PhysicalType::INT32;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       len * sizeof(int32_t));
        memoryUsage += (uint64_t)sizeof(int32_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT64) {
        physical_type_ = PhysicalType::INT64;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       len * sizeof(int64_t));
        memoryUsage += (uint64_t)sizeof(uint64_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT128) {
        physical_type_ = PhysicalType::INT128;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       len * sizeof(int64_t));
        memoryUsage += (uint64_t)sizeof(uint64_t) * len;
    } else {
        throw std::runtime_error(
            "Decimal precision is bigger than the maximum supported width");
    }
}

void DecimalColumnVector::close() {
    if (!closed) {
        ColumnVector::close();
        if (physical_type_ == PhysicalType::INT16 ||
            physical_type_ == PhysicalType::INT32) {
            free(vector);
        }
        vector = nullptr;
    }
}

void DecimalColumnVector::print(int rowCount) {
//    throw InvalidArgumentException("not support print Decimalcolumnvector.");
    for(int i = 0; i < rowCount; i++) {
        std::cout<<vector[i]<<std::endl;
    }
}

DecimalColumnVector::~DecimalColumnVector() {
    if(!closed) {
        DecimalColumnVector::close();
    }
}

void * DecimalColumnVector::current() {
    if(vector == nullptr) {
        return nullptr;
    } else {
        return vector + readIndex;
    }
}

int DecimalColumnVector::getPrecision() {
	return precision;
}


int DecimalColumnVector::getScale() {
	return scale;
}

void DecimalColumnVector::add(std::string &value){
    if(writeIndex > length){
        ensureSize(writeIndex * 2,true);
    }
     // 用于存储转换后的未缩放整数值
    int64_t unscaled_value = 0;

    try {
        size_t dot_pos = value.find('.');

        if (dot_pos != std::string::npos) {
            // 有小数点的情况
            std::string int_part = value.substr(0, dot_pos);  // 获取整数部分
            std::string frac_part = value.substr(dot_pos + 1);  // 获取小数部分
            std::cout<<"int_part: "<<int_part<<" frac_part: "<<frac_part<<std::endl;
            // 检查小数部分长度是否超出精度要求
            if (frac_part.length() > scale) {
                throw std::invalid_argument("Decimal string exceeds scale.");
            }

            // 计算未缩放的整数值
            // 将整数部分与小数部分合并后转换为整数
            unscaled_value = std::stoll(int_part + frac_part);
            std::cout<<"unscaled_value: "<<unscaled_value<<std::endl;
            // 将小数部分补充到精度要求的位数
            for (size_t i = 0; i < scale - frac_part.length(); ++i) {
                unscaled_value *= 10;
            }
            std::cout<<"unscaled_value2: "<<unscaled_value<<std::endl;
        } else {
            // 没有小数点的情况，直接转换为整数
            unscaled_value = std::stoll(value);
        }
    } catch (const std::exception &e) {
        throw std::invalid_argument("Invalid decimal string: " + value);
    }
    // 更新写入索引
    int index = writeIndex++;
    // 根据精度确定物理类型并将值存储到 vector 中
    switch (physical_type_) {
        case PhysicalType::INT16:
            vector[index] = static_cast<int16_t>(unscaled_value);
            break;
        case PhysicalType::INT32:
            vector[index] = static_cast<int32_t>(unscaled_value);
            break;
        case PhysicalType::INT64:
            vector[index] = unscaled_value;
            break;
        case PhysicalType::INT128:
            // INT128 类型的处理，如果需要处理超大值
            // 这里假设存储的是 int64_t 最大范围内的值
            vector[index] = unscaled_value;
            break;
        default:
            throw std::invalid_argument("Unsupported physical type for decimal column");
    }
    isNull[index]=false;
}

void DecimalColumnVector::add(int64_t value){

}

void DecimalColumnVector::ensureSize(uint64_t size, bool preserveData){
    ColumnVector::ensureSize(size, preserveData);
    if (length < size) {
        long *oldVector = vector;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                        size * sizeof(int64_t));
        if (preserveData) {
            std::copy(oldVector, oldVector + length, vector);
        }
        delete[] oldVector;
        memoryUsage += (long) sizeof(long) * (size - length);
        resize(size);
    }
}