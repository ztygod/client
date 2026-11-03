/*
 *     Copyright 2026 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef DRAGONFLY_RDMA_MEMORY_H
#define DRAGONFLY_RDMA_MEMORY_H
#include <stddef.h>
#include <stdint.h>

int d7y_rdma_mr_register(void *domain, void *data, size_t size, uint64_t key,
                    int remote_read, void **result);
void *d7y_rdma_mr_descriptor(void *mr);
uint64_t d7y_rdma_mr_key(void *mr);
uint64_t d7y_rdma_mr_address(uint64_t mode, const void *data);


#endif
