#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from ckan.lib.cli import CkanCommand
from docopt import docopt

import ckanext.cloudstorage.utils as utils

USAGE = """ckanext-cloudstorage
Commands:
    - fix-cors                  Update CORS rules where possible.
    - migrate                   Upload local storage to the remote.
    - initdb                    Reinitalize database tables.
    - list-unlinked-uploads     Lists uploads in the storage container that do not match to any resources.
    - remove-unlinked-uploads   Permanently deletes uploads from the storage container that do not match to any resources.
    - list-missing-uploads      Lists resources IDs that are missing uploads in the storage container.
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
    utils.migrate(path, single_id)


def _fix_cors(args):
    msg, _ = utils.fix_cors(args['<domains>'])
    print(msg)


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
