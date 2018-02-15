#!/usr/bin/env python
# -*-coding:utf-8 -*
import click
import logging
import os
import swiftclient

from alkivi.logger import Logger
from swiftclient.service import SwiftService

# Define the global logger
logger = Logger(min_log_level_to_mail=None,
                min_log_level_to_save=None,
                min_log_level_to_print=logging.INFO,
                min_log_level_to_syslog=None)

DEFAULT_AUTH = 'https://auth.cloud.ovh.net/v2.0/'

# TODO make this dynamic, using number of files ?
ALLOWED_DIFF = 5000


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
@click.option('--path', prompt=True,
              default='.',
              help='Local path where download happen')
def list_size(debug, username, password, authurl, authversion,
              tenantid, tenantname, regionname,
              container, prefix, depth, path):
    """
    Check swift path versus local path
    If a file is missing locally, return 1
    If the remote_size is bigger than ALLOWED_DIFF, return 2
    Otherwise return 0
    """
    if debug:
        logger.set_min_level_to_print(logging.DEBUG)
        logger.set_min_level_to_save(logging.DEBUG)

    local_path = os.path.realpath(path)

    service = get_swift_service(username, password, authurl, authversion,
                                tenantid, tenantname, regionname)

    exit_code = 0
    missing_files = []
    number_to_stop = 20
    max_missing = False

    remote_size = 0
    options = {'prefix': prefix}
    list_parts_gen = service.list(container=container, options=options)
    for page in list_parts_gen:
        if page['success']:
            for item in page['listing']:
                size = item['bytes']
                name = item['name']

                local_name = os.path.join(local_path, name)
                if not os.path.isfile(local_name):
                    logger.info(u'File {0} does not exist locally'.format(name))
                    exit_code = 1

                    if len(missing_files) >= number_to_stop:
                        max_missing = True
                        break
                    else:
                        missing_files.append(name)
                remote_size += size
        else:
            logger.warning('test', page['error'])
        if max_missing:
            break

    segments_container = '{0}_segments'.format(container)
    list_segments_gen = service.list(container=segments_container,
                                     options=options)
    for page in list_segments_gen:
        if page['success']:
            for item in page['listing']:
                size = item['bytes']
                remote_size += size
        else:
            logger.warning('test', page['error'])

    if exit_code:
        logger.info('Helper to for download')
        for f in missing_files:
            logger.info(u'swift {0} download {1}'.format(container, f))

    logger.info(u'Remote size is {0} == {1}'.format(remote_size,
                                                    sizeof_fmt(remote_size)))

    local_path_with_prefix = os.path.join(local_path, prefix)

    if not os.path.isdir(local_path_with_prefix):
        logger.info('Local dir not existing')
        exit(3)

    local_size = get_local_size(local_path_with_prefix)
    diff = remote_size - local_size

    logger.info(u'Local size is {0} == {1}'.format(local_size,
                                                   sizeof_fmt(local_size)))
    logger.info(u'Diff is {0}'.format(diff))

    if exit_code:
        exit(exit_code)
    elif diff < 0:
        exit(0)
    elif diff > ALLOWED_DIFF:
        logger.info('Diff is large, exiting with 2')
        exit(2)
    else:
        logger.info(u'Small diff {0}'.format(diff))
        exit(0)


def get_local_size(folder):
    total_size = os.path.getsize(folder)
    for item in os.listdir(folder):
        itempath = os.path.join(folder, item)
        if os.path.isfile(itempath):
            total_size += os.path.getsize(itempath)
        elif os.path.isdir(itempath):
            total_size += get_local_size(itempath)
    return total_size


if __name__ == "__main__":
    try:
        list_size()
    except Exception as exception:
        logger.exception(exception)
