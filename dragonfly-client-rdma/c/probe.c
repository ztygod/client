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

#define _POSIX_C_SOURCE 200809L

#include "probe.h"

#include <rdma/fabric.h>
#include <stdlib.h>
#include <string.h>

int d7y_rdma_probe(const char *provider, const char *domain, void **result)
{
    struct fi_info *hints;
    struct fi_info *info = NULL;
    int ret;

    if (!provider || !result)
        return -FI_EINVAL;
    *result = NULL;

    hints = fi_allocinfo();
    if (!hints)
        return -FI_ENOMEM;

    hints->caps = FI_MSG | FI_RMA | FI_READ | FI_REMOTE_READ | FI_REMOTE_COMM;
    hints->ep_attr->type = FI_EP_MSG;

    /* Discover requirements, as fi_info does, without claiming to satisfy them. */
    hints->mode = UINT64_MAX;
    hints->domain_attr->mode = UINT64_MAX;
    hints->domain_attr->mr_mode = ~(FI_MR_BASIC | FI_MR_SCALABLE);

    hints->fabric_attr->prov_name = strdup(provider);
    if (!hints->fabric_attr->prov_name) {
        fi_freeinfo(hints);
        return -FI_ENOMEM;
    }
    if (domain) {
        hints->domain_attr->name = strdup(domain);
        if (!hints->domain_attr->name) {
            fi_freeinfo(hints);
            return -FI_ENOMEM;
        }
    }

    ret = fi_getinfo(FI_VERSION(1, 17), NULL, NULL, 0, hints, &info);
    fi_freeinfo(hints);
    if (ret) {
        if (info)
            fi_freeinfo(info);
        return ret == -FI_ENODATA ? 0 : ret;
    }

    *result = info;
    return 0;
}

void d7y_rdma_free_info(void *result)
{
    if (result)
        fi_freeinfo(result);
}

void *d7y_rdma_next_info(const void *info)
{
    return info ? ((const struct fi_info *) info)->next : NULL;
}

int d7y_rdma_get_info(const void *info, struct d7y_rdma_info *result)
{
    const struct fi_info *entry = info;
    if (!entry || !result || !entry->fabric_attr || !entry->domain_attr ||
        !entry->ep_attr || entry->ep_attr->type != FI_EP_MSG)
        return -FI_EINVAL;

    memset(result, 0, sizeof(*result));
    result->provider = entry->fabric_attr->prov_name;
    result->domain = entry->domain_attr->name;
    if (entry->nic && entry->nic->device_attr)
        result->device = entry->nic->device_attr->name;
    result->capabilities = entry->caps;
    result->mode = entry->mode;
    result->mr_mode = entry->domain_attr->mr_mode;
    result->protocol = entry->ep_attr->protocol;
    result->protocol_version = entry->ep_attr->protocol_version;
    result->threading = entry->domain_attr->threading;
    result->control_progress = entry->domain_attr->control_progress;
    result->data_progress = entry->domain_attr->data_progress;
    result->messaging = (entry->caps & FI_MSG) != 0;
    result->rma = (entry->caps & FI_RMA) != 0;
    result->read = (entry->caps & FI_READ) != 0;
    result->remote_read = (entry->caps & FI_REMOTE_READ) != 0;
    result->remote_communication = (entry->caps & FI_REMOTE_COMM) != 0;
    return 0;
}

const char *d7y_rdma_error_string(int code)
{
    return fi_strerror(code < 0 ? -code : code);
}
