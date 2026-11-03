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
#include "endpoint.h"
#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_eq.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#define SESSION_CAPS (FI_MSG | FI_RMA | FI_READ | FI_REMOTE_READ | FI_REMOTE_COMM)
#define SESSION_MODE (FI_CONTEXT | FI_LOCAL_MR | FI_RX_CQ_DATA)
#define SESSION_MR (FI_MR_LOCAL | FI_MR_VIRT_ADDR | FI_MR_ALLOCATED | FI_MR_PROV_KEY)

int d7y_rdma_session_validate(const void *value)
{
    const struct fi_info *i = value;
    if (!i || !i->ep_attr || !i->domain_attr || !i->tx_attr || !i->rx_attr ||
        !i->fabric_attr || i->ep_attr->type != FI_EP_MSG ||
        (i->caps & SESSION_CAPS) != SESSION_CAPS ||
        (i->mode & ~SESSION_MODE) ||
        (i->tx_attr->mode & ~SESSION_MODE) ||
        (i->rx_attr->mode & ~SESSION_MODE) ||
        (i->domain_attr->mode & ~SESSION_MODE) ||
        (i->domain_attr->mr_mode & ~(SESSION_MR | FI_MR_BASIC)) ||
        !i->ep_attr->max_msg_size)
        return -FI_ENOSYS;
    return 0;
}

int d7y_rdma_session_query(const char *provider, const char *domain, const char *node,
                     const char *service, int source, void **result)
{
    struct fi_info *hints = fi_allocinfo(), *list = NULL, *i;
    int ret;
    if (!hints)
        return -FI_ENOMEM;
    *result = NULL;
    hints->caps = SESSION_CAPS;
    hints->mode = SESSION_MODE;
    hints->ep_attr->type = FI_EP_MSG;
    hints->domain_attr->mr_mode = SESSION_MR;
    hints->domain_attr->threading = FI_THREAD_DOMAIN;
    hints->fabric_attr->prov_name = strdup(provider);
    if (domain)
        hints->domain_attr->name = strdup(domain);
    if (!hints->fabric_attr->prov_name || (domain && !hints->domain_attr->name)) {
        fi_freeinfo(hints);
        return -FI_ENOMEM;
    }
    ret = fi_getinfo(FI_VERSION(1, 17), node, service, source ? FI_SOURCE : 0,
                     hints, &list);
    fi_freeinfo(hints);
    if (ret)
        return ret;
    ret = -FI_ENODATA;
    for (i = list; i; i = i->next) {
        if (strcmp(i->fabric_attr->prov_name, provider) ||
            (domain && strcmp(i->domain_attr->name, domain)))
            continue;
        ret = d7y_rdma_session_validate(i);
        if (ret)
            continue;
        *result = fi_dupinfo(i);
        ret = *result ? 0 : -FI_ENOMEM;
        break;
    }
    fi_freeinfo(list);
    return ret;
}

size_t d7y_rdma_session_max_size(const void *i)
{
    return ((const struct fi_info *)i)->ep_attr->max_msg_size;
}
uint64_t d7y_rdma_session_mr_mode(const void *i)
{
    return ((const struct fi_info *)i)->domain_attr->mr_mode;
}
void *d7y_rdma_session_dest(const void *i)
{
    return ((const struct fi_info *)i)->dest_addr;
}

int d7y_rdma_eq_open(void *fabric, void **result)
{
    struct fi_eq_attr attr = {0};
    struct fid_eq *eq = NULL;
    int ret;
    attr.wait_obj = FI_WAIT_NONE;
    ret = fi_eq_open(fabric, &attr, &eq, NULL);
    *result = eq;
    return ret;
}
int d7y_rdma_pep_open(void *fabric, const void *info, void **result)
{
    struct fid_pep *pep = NULL;
    int ret = fi_passive_ep(fabric, (struct fi_info *)info, &pep, NULL);
    *result = pep;
    return ret;
}
int d7y_rdma_pep_listen(void *pep, void *eq)
{
    int ret = fi_pep_bind(pep, &((struct fid_eq *)eq)->fid, 0);
    return ret ? ret : fi_listen(pep);
}
int d7y_rdma_ep_open(void *domain, const void *info, void **result)
{
    struct fid_ep *ep = NULL;
    int ret = fi_endpoint(domain, (struct fi_info *)info, &ep, NULL);
    *result = ep;
    return ret;
}
int d7y_rdma_ep_bind_enable(void *ep, void *eq, void *tx, void *rx)
{
    int ret = fi_ep_bind(ep, &((struct fid_eq *)eq)->fid, 0);
    if (!ret) ret = fi_ep_bind(ep, &((struct fid_cq *)tx)->fid, FI_TRANSMIT);
    if (!ret) ret = fi_ep_bind(ep, &((struct fid_cq *)rx)->fid, FI_RECV);
    return ret ? ret : fi_enable(ep);
}
int d7y_rdma_ep_connect(void *ep, const void *address)
{
    return fi_connect(ep, address, NULL, 0);
}
int d7y_rdma_ep_accept(void *ep)
{
    return fi_accept(ep, NULL, 0);
}
int d7y_rdma_ep_shutdown(void *ep)
{
    return fi_shutdown(ep, 0);
}
int d7y_rdma_pep_reject(void *pep, const void *info)
{
    return fi_reject(pep, ((const struct fi_info *)info)->handle, NULL, 0);
}

int d7y_rdma_eq_poll(void *eq, uint32_t *event, void **fid, void **info,
                int *provider_error, char *diagnostic, size_t diagnostic_size)
{
    /* The protocol sends no CM private data. Extra data is rejected by libfabric. */
    struct fi_eq_cm_entry entry = {0};
    ssize_t ret = fi_eq_read(eq, event, &entry, sizeof(entry), 0);
    *info = NULL;
    *fid = NULL;
    *provider_error = 0;
    if (diagnostic_size) diagnostic[0] = '\0';
    if (ret == -FI_EAGAIN) return 0;
    if (ret == -FI_EAVAIL) {
        struct fi_eq_err_entry error = {0};
        ret = fi_eq_readerr(eq, &error, 0);
        if (ret < 0) return (int)ret;
        *provider_error = error.prov_errno;
        if (diagnostic_size) {
            const char *detail = fi_eq_strerror(eq, error.prov_errno,
                                                   error.err_data, diagnostic, diagnostic_size);
            if (detail && detail != diagnostic)
                snprintf(diagnostic, diagnostic_size, "%s", detail);
            diagnostic[diagnostic_size - 1] = '\0';
        }
        return error.err ? -error.err : -FI_EIO;
    }
    if (ret < 0) return (int)ret;
    if ((size_t)ret < sizeof(entry)) return -FI_EIO;
    *fid = entry.fid;
    *info = entry.info;
    return 1;
}
int d7y_rdma_is_shutdown(uint32_t event)
{
    return event == FI_SHUTDOWN;
}
int d7y_rdma_is_connected(uint32_t event)
{
    return event == FI_CONNECTED;
}
int d7y_rdma_is_connreq(uint32_t event)
{
    return event == FI_CONNREQ;
}
