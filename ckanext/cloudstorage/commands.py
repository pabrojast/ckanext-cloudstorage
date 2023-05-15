#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import os
import os.path
import cgi
import tempfile

from docopt import docopt

from ckan.lib.cli import CkanCommand
from docopt import docopt

from ckanapi import LocalCKAN
from ckanext.cloudstorage.storage import (
    CloudStorage,
    ResourceCloudStorage
)

import ckanext.cloudstorage.utils as utils

from ckan.logic import NotFound

USAGE = """ckanext-cloudstorage
Commands:
    - fix-cors                  Update CORS rules where possible.
    - migrate                   Upload local storage to the remote.
    - initdb                    Reinitalize database tables.
    - list-unlinked-uploads     Lists uploads in the storage container that do not match to any resources.
    - remove-unlinked-uploads   Permanently deletes uploads from the storage container that do not match to any resources.
    - list-missing-uploads      Lists resources that are missing uploads in the storage container.
    - list-linked-uploads       Lists uploads in the storage container that do match to a resource.

Usage:
    cloudstorage fix-cors <domains>... [--c=<config>]
    cloudstorage migrate <path_to_storage> [<resource_id>] [--c=<config>]
    cloudstorage initdb [--c=<config>]
    cloudstorage list-unlinked-uploads [--o=<output>] [--c=<config>]
    cloudstorage remove-unlinked-uploads [--c=<config>]
    cloudstorage list-missing-uploads [--o=<output>] [--c=<config>]
    cloudstorage list-linked-uploads [--o=<output>] [--c=<config>]

Options:
    -c=<config>       The CKAN configuration file.
    -o=<output>       The output file path.
"""


class FakeFileStorage(cgi.FieldStorage):
    def __init__(self, fp, filename):
        self.file = fp
        self.filename = filename


class PasterCommand(CkanCommand):
    summary = 'ckanext-cloudstorage maintence utilities.'
    usage = USAGE

    def __init__(self, name):
        super(PasterCommand, self).__init__(name)
        self.parser.add_option('-o', '--output', dest='output', action='store',
                               default=None, help='The output file path.')

    def command(self):
        self._load_config()
        args = docopt(USAGE, argv=self.args)

        if args['fix-cors']:
            _fix_cors(args)
        elif args['migrate']:
            _migrate(args)
        elif args['initdb']:
            _initdb()
        elif args['list-unlinked-uploads']:
            _list_unlinked_uploads(self.options.output)
        elif args['remove-unlinked-uploads']:
            _remove_unlinked_uploads()
        elif args['list-missing-uploads']:
            _list_missing_uploads(self.options.output)
        elif args['list-linked-uploads']:
            _list_linked_uploads(self.options.output)


def _migrate(args):
    path = args['<path_to_storage>']
    single_id = args['<resource_id>']
    if not os.path.isdir(path):
        print('The storage directory cannot be found.')
        return

    lc = LocalCKAN()
    resources = {}
    failed = []

    # The resource folder is stuctured like so on disk:
    # - storage/
    #   - ...
    # - resources/
    #   - <3 letter prefix>
    #     - <3 letter prefix>
    #       - <remaining resource_id as filename>
    #       ...
    #     ...
    #   ...
    for root, dirs, files in os.walk(path):
        # Only the bottom level of the tree actually contains any files. We
        # don't care at all about the overall structure.
        if not files:
            continue

        split_root = root.split('/')
        resource_id = split_root[-2] + split_root[-1]

        for file_ in files:
            ckan_res_id = resource_id + file_
            if single_id and ckan_res_id != single_id:
                continue

            resources[ckan_res_id] = os.path.join(
                root,
                file_
            )

    for i, resource in enumerate(resources.iteritems(), 1):
        resource_id, file_path = resource
        print('[{i}/{count}] Working on {id}'.format(
            i=i,
            count=len(resources),
            id=resource_id
        ))

        try:
            resource = lc.action.resource_show(id=resource_id)
        except NotFound:
            print(u'\tResource not found')
            continue

        if resource['url_type'] != 'upload':
            print(u'\t`url_type` is not `upload`. Skip')
            continue

        with open(file_path, 'rb') as fin:
            resource['upload'] = FakeFileStorage(
                fin,
                resource['url'].split('/')[-1]
            )
            try:
                uploader = ResourceCloudStorage(resource)
                uploader.upload(resource['id'])
            except Exception as e:
                failed.append(resource_id)
                print(u'\tError of type {0} during upload: {1}'.format(type(e), e))

    if failed:
        log_file = tempfile.NamedTemporaryFile(delete=False)
        log_file.file.writelines(failed)
        print(u'ID of all failed uploads are saved to `{0}`'.format(log_file.name))


def _fix_cors(args):
    cs = CloudStorage()

    if cs.can_use_advanced_azure:
        from azure.storage import blob as azure_blob
        from azure.storage import CorsRule

        blob_service = azure_blob.BlockBlobService(
            cs.driver_options['key'],
            cs.driver_options['secret']
        )

        blob_service.set_blob_service_properties(
            cors=[
                CorsRule(
                    allowed_origins=args['<domains>'],
                    allowed_methods=['GET']
                )
            ]
        )
        print('Done!')
    else:
        print(
            'The driver {driver_name} being used does not currently'
            ' support updating CORS rules through'
            ' cloudstorage.'.format(
                driver_name=cs.driver_name
            )
        )


def _initdb():
    utils.initdb()


def _list_unlinked_uploads(output_path):
    # type: (str|None) -> None
    utils.list_unlinked_uploads(output_path)


def _remove_unlinked_uploads():
    utils.remove_unlinked_uploads()


def _list_missing_uploads(output_path):
    # type: (str|None) -> None
    utils.list_missing_uploads(output_path)


def _list_linked_uploads(output_path):
    # type: (str|None) -> None
    utils.list_linked_uploads(output_path)
