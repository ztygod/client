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

#ifndef DRAGONFLY_RDMA_TRANSFER_H
#define DRAGONFLY_RDMA_TRANSFER_H
#include <stddef.h>
#include <stdint.h>

void *d7y_rdma_context_alloc(void);
void d7y_rdma_context_free(void *context);
int d7y_rdma_post_send(void *ep, void *data, size_t size, void *desc, void *context);
int d7y_rdma_post_recv(void *ep, void *data, size_t size, void *desc, void *context);
int d7y_rdma_post_read(void *ep, void *data, size_t size, void *desc,
                  uint64_t address, uint64_t key, void *context);
/* 0 = empty, 1 = success, negative = error, including decoded CQ errors. */
int d7y_rdma_cq_poll(void *cq, void **context, size_t *length, uint64_t *flags,
                int *provider_error, char *diagnostic, size_t diagnostic_size);
int d7y_rdma_is_again(int code);
int d7y_rdma_completion_matches(uint64_t flags, int operation);


#endif
