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

#include "memory.h"
#include <rdma/fabric.h>
#include <rdma/fi_domain.h>

int d7y_rdma_mr_register(void *domain, void *data, size_t size, uint64_t key,
                    int remote_read, void **result)
{
    struct fid_mr *mr = NULL;
    uint64_t access = FI_SEND | FI_RECV | FI_READ;
    int ret;
    if (remote_read) access |= FI_REMOTE_READ;
    ret = fi_mr_reg(domain, data, size, access, 0, key, 0, &mr, NULL);
    *result = mr;
    return ret;
}
void *d7y_rdma_mr_descriptor(void *mr)
{
    return fi_mr_desc(mr);
}
uint64_t d7y_rdma_mr_key(void *mr)
{
    return fi_mr_key(mr);
}
uint64_t d7y_rdma_mr_address(uint64_t mode, const void *data)
{
    return (mode & (FI_MR_VIRT_ADDR | FI_MR_BASIC)) ? (uint64_t)(uintptr_t)data : 0;
}
