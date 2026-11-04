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

#include "transfer.h"
#include <rdma/fabric.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_eq.h>
#include <stdlib.h>
#include <stdio.h>

void *d7y_rdma_context_alloc(void)
{
    return calloc(1, sizeof(struct fi_context));
}
void d7y_rdma_context_free(void *context)
{
    free(context);
}
int d7y_rdma_post_send(void *ep, void *data, size_t size, void *desc, void *context)
{
    return (int)fi_send(ep, data, size, desc, FI_ADDR_UNSPEC, context);
}
int d7y_rdma_post_recv(void *ep, void *data, size_t size, void *desc, void *context)
{
    return (int)fi_recv(ep, data, size, desc, FI_ADDR_UNSPEC, context);
}
int d7y_rdma_post_read(void *ep, void *data, size_t size, void *desc,
                  uint64_t address, uint64_t key, void *context)
{
    return (int)fi_read(ep, data, size, desc, FI_ADDR_UNSPEC, address, key, context);
}

int d7y_rdma_cq_poll_many(void *cq, struct d7y_rdma_completion *entries, size_t capacity,
                int *provider_error, char *diagnostic, size_t diagnostic_size)
{
    /* len/buf/data are not defined for every operation kind. Initialize all
     * fields before copying the stable ABI, even when a provider omits them. */
    struct fi_cq_data_entry native[D7Y_RDMA_CQ_BATCH_MAX] = {0};
    ssize_t ret;
    size_t i;
    *provider_error = 0;
    if (diagnostic_size) diagnostic[0] = '\0';
    if (!entries || !capacity || capacity > D7Y_RDMA_CQ_BATCH_MAX) return -FI_EINVAL;
    ret = fi_cq_read(cq, native, capacity);
    if (ret == -FI_EAGAIN) return 0;
    if (ret == -FI_EAVAIL) {
        struct fi_cq_err_entry error = {0};
        ret = fi_cq_readerr(cq, &error, 0);
        if (ret < 0) return (int)ret;
        *provider_error = error.prov_errno;
        if (diagnostic_size) {
            const char *detail = fi_cq_strerror(cq, error.prov_errno,
                                                   error.err_data, diagnostic, diagnostic_size);
            if (detail && detail != diagnostic)
                snprintf(diagnostic, diagnostic_size, "%s", detail);
            diagnostic[diagnostic_size - 1] = '\0';
        }
        return error.err ? -error.err : -FI_EIO;
    }
    if (ret < 0) return (int)ret;
    if ((size_t)ret > capacity) return -FI_EIO;
    for (i = 0; i < (size_t)ret; i++) {
        entries[i].context = native[i].op_context;
        entries[i].length = native[i].len;
        entries[i].flags = native[i].flags;
    }
    return (int)ret;
}
int d7y_rdma_cq_poll(void *cq, void **context, size_t *length, uint64_t *flags,
                int *provider_error, char *diagnostic, size_t diagnostic_size)
{
    struct d7y_rdma_completion entry = {0};
    int ret = d7y_rdma_cq_poll_many(cq, &entry, 1, provider_error, diagnostic, diagnostic_size);
    *context = entry.context;
    *length = entry.length;
    *flags = entry.flags;
    return ret;
}
int d7y_rdma_is_again(int code)
{
    return code == -FI_EAGAIN;
}
int d7y_rdma_completion_matches(uint64_t flags, int operation)
{
    uint64_t required = operation == 0 ? FI_SEND : operation == 1 ? FI_RECV : FI_READ;
    return (flags & required) == required;
}
