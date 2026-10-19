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

#include "resource.h"

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>

int d7y_rdma_fabric_open(const void *info, void **result)
{
    const struct fi_info *entry = info;
    struct fid_fabric *fabric = NULL;
    int ret;

    if (!entry || !entry->fabric_attr || !result)
        return -FI_EINVAL;
    *result = NULL;
    ret = fi_fabric(entry->fabric_attr, &fabric, NULL);
    if (!ret)
        *result = &fabric->fid;
    return ret;
}

int d7y_rdma_domain_open(void *fabric, const void *info, void **result)
{
    struct fid_domain *domain = NULL;
    int ret;

    if (!fabric || !info || !result)
        return -FI_EINVAL;
    *result = NULL;
    ret = fi_domain(fabric, (struct fi_info *) info, &domain, NULL);
    if (!ret)
        *result = &domain->fid;
    return ret;
}

int d7y_rdma_cq_open(void *domain, size_t size, void **result)
{
    struct fid_cq *cq = NULL;
    struct fi_cq_attr attr = {0};
    int ret;

    if (!domain || !result)
        return -FI_EINVAL;
    *result = NULL;
    attr.size = size;
    attr.format = FI_CQ_FORMAT_DATA;
    attr.wait_obj = FI_WAIT_NONE;
    ret = fi_cq_open(domain, &attr, &cq, NULL);
    if (!ret)
        *result = &cq->fid;
    return ret;
}

int d7y_rdma_close(void *resource)
{
    return resource ? fi_close(resource) : 0;
}

void *d7y_rdma_dup_info(const void *info)
{
    return info ? fi_dupinfo(info) : NULL;
}

