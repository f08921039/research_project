/*********copy from linux kernel 6.7.5**************/

/*
 * xxHash - Extremely Fast Hash algorithm
 * Copyright (C) 2012-2016, Yann Collet.
 *
 * BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *   * Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above
 *     copyright notice, this list of conditions and the following disclaimer
 *     in the documentation and/or other materials provided with the
 *     distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License version 2 as published by the
 * Free Software Foundation. This program is dual-licensed; you may select
 * either version 2 of the GNU General Public License ("GPL") or BSD license
 * ("BSD").
 *
 * You can contact the author at:
 * - xxHash homepage: https://cyan4973.github.io/xxHash/
 * - xxHash source repository: https://github.com/Cyan4973/xxHash
 */

/*
 * Notice extracted from xxHash homepage:
 *
 * xxHash is an extremely fast Hash algorithm, running at RAM speed limits.
 * It also successfully passes all tests from the SMHasher suite.
 *
 * Comparison (single thread, Windows Seven 32 bits, using SMHasher on a Core 2
 * Duo @3GHz)
 *
 * Name            Speed       Q.Score   Author
 * xxHash          5.4 GB/s     10
 * CrapWow         3.2 GB/s      2       Andrew
 * MumurHash 3a    2.7 GB/s     10       Austin Appleby
 * SpookyHash      2.0 GB/s     10       Bob Jenkins
 * SBox            1.4 GB/s      9       Bret Mulvey
 * Lookup3         1.2 GB/s      9       Bob Jenkins
 * SuperFastHash   1.2 GB/s      1       Paul Hsieh
 * CityHash64      1.05 GB/s    10       Pike & Alakuijala
 * FNV             0.55 GB/s     5       Fowler, Noll, Vo
 * CRC32           0.43 GB/s     9
 * MD5-32          0.33 GB/s    10       Ronald L. Rivest
 * SHA1-32         0.28 GB/s    10
 *
 * Q.Score is a measure of quality of the hash function.
 * It depends on successfully passing SMHasher test set.
 * 10 is a perfect score.
 *
 * A 64-bits version, named xxh64 offers much better speed,
 * but for 64-bits applications only.
 * Name     Speed on 64 bits    Speed on 32 bits
 * xxh64       13.8 GB/s            1.9 GB/s
 * xxh32        6.8 GB/s            6.0 GB/s
 */

#ifndef XXHASH_H
#define XXHASH_H

#include "compiler.h"

/*-****************************
 * Simple Hash Functions
 *****************************/

/**
 * xxh32() - calculate the 32-bit hash of the input with a given seed.
 *
 * @input:  The data to hash.
 * @length: The length of the data to hash.
 * @seed:   The seed can be used to alter the result predictably.
 *
 * Speed on Core 2 Duo @ 3 GHz (single thread, SMHasher benchmark) : 5.4 GB/s
 *
 * Return:  The 32-bit hash of the data.
 */
uint32_t xxh32(const void *input, size_t length, uint32_t seed);

/**
 * xxh64() - calculate the 64-bit hash of the input with a given seed.
 *
 * @input:  The data to hash.
 * @length: The length of the data to hash.
 * @seed:   The seed can be used to alter the result predictably.
 *
 * This function runs 2x faster on 64-bit systems, but slower on 32-bit systems.
 *
 * Return:  The 64-bit hash of the data.
 */
uint64_t xxh64(const void *input, size_t length, uint64_t seed);



static inline unsigned long prehash64(const void *input, 
                        size_t length, uint64_t seed) {
    return xxh64(input, length, seed);
}

static inline unsigned long prehash32(const void *input, 
                        size_t length, uint64_t seed) {
    return xxh32(input, length, seed);
}


#endif /* XXHASH_H */
