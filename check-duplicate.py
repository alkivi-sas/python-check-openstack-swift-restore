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
@click.option('--prefix', default=None,
              help='Path to look (without leading /)')
@click.option('--erase', default=False, is_flag=True,
              help='Perform delete')
def check_duplicate(debug, username, password, authurl, authversion,
              tenantid, tenantname, regionname,
              container, prefix, erase):
    """
    Print remote data size by depth
    """
    if debug:
        logger.set_min_level_to_print(logging.DEBUG)
        logger.set_min_level_to_save(logging.DEBUG)

    service = get_swift_service(username, password, authurl, authversion,
                                tenantid, tenantname, regionname)

    data = {}
    total = 0

    logger.info('starting container check')
    options = {}
    if prefix:
        options['prefix'] = prefix
    list_parts_gen = service.list(container=container, options=options)
    deleted = 0
    for page in list_parts_gen:
        logger.info('new page')
        if page['success']:
            for item in page['listing']:
                size = item['bytes']
                name = item['name']
                logger.debug('Investigating {0}'.format(name.encode('utf-8')))

                test_name = name.split('/')[-1]
                to_find_name = u'{0}/{1}'.format(name, test_name)
                duplicate = get_object_stat(service, container, to_find_name)
                if duplicate:
                    source = get_object_stat(service, container, name)
                    is_equal = handle_duplicate(container, service, source, duplicate)
                    if is_equal:
                        logger.debug('Found a duplicate', source, duplicate)
                        to_delete = duplicate['headers']['content-length']
                        deleted += int(to_delete)
                        if erase:
                            del_iter = service.delete(container=container, objects=[to_find_name])
                            for del_res in del_iter:
                                c = del_res.get('container', '')
                                o = del_res.get('object', '')
                                a = del_res.get('attempts')
                                if del_res['success'] and not del_res['action'] == 'bulk_delete':
                                    rd = del_res.get('response_dict')
                                    if rd is not None:
                                        logger.info(
                                            'Successfully deleted {0}/{1} in {2} '
                                            'attempts'.format(c, o.encode('utf-8'), a)
                                        )
                        else:
                            logger.info('Would have deleted {0} : {1}'.format(to_find_name.encode('utf-8'), to_delete.encode('utf-8')))

                    else:
                        logger.warning('Not a duplicate ?', source, duplicate)
        else:
            logger.warning('test', page['error'])

    if erase:
        logger.info('I have delete {0} == {1}'.format(deleted, sizeof_fmt(deleted)))
    else:
        logger.info('Would have delete {0} == {1}'.format(deleted, sizeof_fmt(deleted)))

def handle_duplicate(container, service, source, duplicate):
    is_equal = True
    for header in ['content-length', 'etag', 'content-type']:
        if source['headers'][header] != duplicate['headers'][header]:
            is_equal = False

    return is_equal

def get_object_stat(service, container, name):
    test = service.stat(container=container, objects=[name])
    item = None
    for data in test:
        if data['success']:
            if not item:
                item = data
            else:
                logger.warning('Multiple entries with name {0}'.format(name.encode('utf-8')))
                return None
    return item


if __name__ == "__main__":
    try:
        check_duplicate()
    except Exception as exception:
        logger.exception(exception)
