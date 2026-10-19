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

#ifndef DRAGONFLY_RDMA_PROBE_H
#define DRAGONFLY_RDMA_PROBE_H

#include <stdint.h>

/* Strings borrow from the query result and must be copied before it is freed. */
struct d7y_rdma_info {
    const char *provider;
    const char *domain;
    const char *device;
    uint64_t capabilities;
    uint64_t mode;
    uint64_t mr_mode;
    uint32_t protocol;
    uint32_t protocol_version;
    uint32_t threading;
    uint32_t control_progress;
    uint32_t data_progress;
    uint8_t messaging;
    uint8_t rma;
    uint8_t read;
    uint8_t remote_read;
    uint8_t remote_communication;
};

/* Success with a NULL result means no matching interface. */
int d7y_rdma_probe(const char *provider, const char *domain, void **result);
void d7y_rdma_free_info(void *result);
void *d7y_rdma_next_info(const void *info);
int d7y_rdma_get_info(const void *info, struct d7y_rdma_info *result);
const char *d7y_rdma_error_string(int code);

#endif
