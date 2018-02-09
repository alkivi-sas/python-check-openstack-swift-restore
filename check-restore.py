#!/usr/bin/env python
# -*-coding:utf-8 -*

import atexit
import click
import logging
import os
import swiftclient

from scriptlock import Lock
from alkivi.logger import Logger
from swiftclient.service import SwiftService

# Define the global logger
logger = Logger(min_log_level_to_mail=None,
                min_log_level_to_save=None,
                min_log_level_to_print=logging.INFO,
                min_log_level_to_syslog=None)

LOCK = Lock()
atexit.register(LOCK.cleanup)

DEFAULT_AUTH = 'https://auth.cloud.ovh.net/v2.0/'


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def get_swift_client(username, password, authurl, authversion,
                     tenantid, tenantname, regionname):
    """Take the conf and return storage_url and token."""

    return swiftclient.Connection(authurl, username, password,
                                  auth_version=authversion,
                                  os_options={
                                      'tenant_name': tenantname,
                                      'region_name': regionname
                                  })


def get_swift_service(username, password, authurl, authversion,
                      tenantid, tenantname, regionname):
    """Take the conf and return storage_url and token."""

    options = {'auth_version': authversion,
               'user': username,
               'key': password,
               'auth': authurl,
               'auth_url': authurl,
               'tenant_id': tenantid,
               'tenant_name': tenantname,
               'os_tenant_name': tenantname,
               'os_tenant_id': tenantid,
               'os_region_name': regionname,
               'os_username': username,
               'os_password': password}

    return SwiftService(options=options)


@click.command()
@click.option('--debug', default=False, is_flag=True,
              help='Toggle Debug mode')
@click.option('--username',
              default=lambda: os.environ.get('OS_USERNAME', ''),
              help='Swift username')
@click.option('--password',
              default=lambda: os.environ.get('OS_PASSWORD', ''),
              help='Swift password')
@click.option('--authurl',
              default=lambda: os.environ.get('OS_AUTH_URL',
                                             DEFAULT_AUTH),
              help='Swift Auth URL')
@click.option('--authversion',
              default=2,
              help='Swift Auth Version')
@click.option('--tenantid',
              default=lambda: os.environ.get('OS_TENANT_ID', ''),
              help='swift tenant id')
@click.option('--tenantname',
              default=lambda: os.environ.get('OS_TENANT_NAME', ''),
              help='swift tenant name')
@click.option('--regionname',
              default=lambda: os.environ.get('OS_REGION_NAME', 'SBG1'),
              help='Swift Region Name')
@click.option('--container', prompt=True,
              help='Swift Container to Look')
@click.option('--prefix', prompt=True,
              default='PROD/TEST',
              help='Path to look (without leading /)')
@click.option('--depth', prompt=True,
              default=1,
              help='Print size by depth')
def list_size(debug, username, password, authurl, authversion,
              tenantid, tenantname, regionname,
              container, prefix, depth):
    """
    Monitor all backup data on a minion.

    Check duplicity last full backup using log
    Check object storage date
    """
    if debug:
        logger.set_min_level_to_print(logging.DEBUG)
        logger.set_min_level_to_save(logging.DEBUG)

    service = get_swift_service(username, password, authurl, authversion,
                                tenantid, tenantname, regionname)

    data = {}
    options = {'prefix': prefix}
    list_parts_gen = service.list(container=container, options=options)
    for page in list_parts_gen:
        if page['success']:
            for item in page['listing']:
                size = item['bytes']
                wanted_path = '/'.join(item['name'].split('/')[0:depth])
                if wanted_path not in data:
                    data[wanted_path] = 0
                data[wanted_path] += size
        else:
            logger.warning('test', page['error'])

    for path, size in data.items():
        logger.info('Size of {0} is {1} = {2}'.format(path,
                                                      size,
                                                      sizeof_fmt(size)))
    exit(0)


if __name__ == "__main__":
    try:
        list_size()
    except Exception as exception:
        logger.exception(exception)
