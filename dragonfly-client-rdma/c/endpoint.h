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

#ifndef DRAGONFLY_RDMA_ENDPOINT_H
#define DRAGONFLY_RDMA_ENDPOINT_H
#include <stddef.h>
#include <stdint.h>

/* Created handles and query/CM info are owned. session_dest borrows from info. */
int d7y_rdma_session_query(const char *provider, const char *domain, const char *node,
                     const char *service, int source, void **result);
int d7y_rdma_session_validate(const void *info);
size_t d7y_rdma_session_max_size(const void *info);
uint64_t d7y_rdma_session_mr_mode(const void *info);
void *d7y_rdma_session_dest(const void *info);
int d7y_rdma_eq_open(void *fabric, void **result);
int d7y_rdma_pep_open(void *fabric, const void *info, void **result);
int d7y_rdma_pep_listen(void *pep, void *eq);
int d7y_rdma_ep_open(void *domain, const void *info, void **result);
int d7y_rdma_ep_bind_enable(void *ep, void *eq, void *tx, void *rx);
int d7y_rdma_ep_connect(void *ep, const void *address);
int d7y_rdma_ep_accept(void *ep);
int d7y_rdma_ep_shutdown(void *ep);
int d7y_rdma_pep_reject(void *pep, const void *info);
/* 0 = no event, 1 = event, negative = native error. CM info is owned. */
int d7y_rdma_eq_poll(void *eq, uint32_t *event, void **fid, void **info,
                int *provider_error, char *diagnostic, size_t diagnostic_size);

int d7y_rdma_is_shutdown(uint32_t event);
int d7y_rdma_is_connected(uint32_t event);
int d7y_rdma_is_connreq(uint32_t event);

#endif
