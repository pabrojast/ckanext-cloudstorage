#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import os.path
import cgi
import tempfile
import click

from docopt import docopt
from ckan.lib.cli import CkanCommand
from ckan.lib.munge import munge_filename
from ckan import model

from ckanapi import LocalCKAN
from ckanext.cloudstorage.storage import (
    CloudStorage,
    ResourceCloudStorage
)
from ckanext.cloudstorage.model import (
    create_tables,
    drop_tables
)
from ckan.logic import NotFound

USAGE = """ckanext-cloudstorage

Commands:
    - fix-cors                  Update CORS rules where possible.
    - migrate                   Upload local storage to the remote.
    - initdb                    Reinitalize database tables.
    - list-unlinked-uploads     Lists uploads in the storage container that do not match to any resources.
    - remove-unlinked-uploads   Permanently deletes uploads from the storage container that do not match to any resources.
    - list-missing-uploads      Lists resources IDs that are missing uploads in the storage container.

Usage:
    cloudstorage fix-cors <domains>... [--c=<config>]
    cloudstorage migrate <path_to_storage> [<resource_id>] [--c=<config>]
    cloudstorage initdb [--c=<config>]
    cloudstorage list-unlinked-uploads [--c=<config>]
    cloudstorage remove-unlinked-uploads [--c=<config>]
    cloudstorage list-missing-uploads [--c=<config>]

Options:
    -c=<config>       The CKAN configuration file.
"""


class FakeFileStorage(cgi.FieldStorage):
    def __init__(self, fp, filename):
        self.file = fp
        self.filename = filename


class PasterCommand(CkanCommand):
    summary = 'ckanext-cloudstorage maintence utilities.'
    usage = USAGE

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
            _list_unlinked_uploads()
        elif args['remove-unlinked-uploads']:
            _remove_unlinked_uploads()
        elif args['list-missing-uploads']:
            _list_missing_uploads()


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


def _get_unlinked_uploads(return_objects = False):
    cs = CloudStorage()

    resource_urls = []
    resource_ids_and_filenames = model.Session.query(
                                    model.Resource.id,
                                    model.Resource.url) \
                                 .filter(model.Resource.url_type == 'upload') \
                                 .all()

    for id, filename in resource_ids_and_filenames:
        resource_urls.append(os.path.join(
                                'resources',
                                id,
                                munge_filename(filename)))

    uploads = cs.container.list_objects()

    uploads_missing_resources = []
    for upload in uploads:
        if upload.name not in resource_urls:
            uploads_missing_resources.append(
                upload if return_objects else upload.name)

    return uploads_missing_resources


def _list_unlinked_uploads():
    uploads_missing_resources = _get_unlinked_uploads()

    if uploads_missing_resources:
        click.echo(uploads_missing_resources)

    click.echo(u"Found {} upload(s) with missing or deleted resources."
                .format(len(uploads_missing_resources)))


def _remove_unlinked_uploads():
    cs = CloudStorage()

    uploads_missing_resources = _get_unlinked_uploads(return_objects = True)

    num_success = 0
    num_failures = 0
    for upload in uploads_missing_resources:
        if cs.container.delete_object(upload):
            click.echo(u"Deleted {}".format(upload.name))
            num_success += 1
        else:
            click.echo(u"Failed to delete {}".format(upload.name))
            num_failures += 1

    if num_success:
        click.echo(u"Deleted {} upload(s).".format(num_success))

    if num_failures:
        click.echo(u"Failed to delete {} upload(s).".format(num_failures))


def _list_missing_uploads():
    cs = CloudStorage()

    upload_urls = set(u.name for u in cs.container.list_objects())

    resource_ids_and_filenames = model.Session.query(
                                    model.Resource.id,
                                    model.Resource.url) \
                                 .filter(model.Resource.url_type == u'upload') \
                                 .all()

    resource_ids_missing_uploads = []
    for id, filename in resource_ids_and_filenames:
        url = os.path.join(
                'resources',
                id,
                munge_filename(filename))

        if url not in upload_urls:
            resource_ids_missing_uploads.append(id)

    if resource_ids_missing_uploads:
        click.echo(resource_ids_missing_uploads)

    click.echo(u"Found {} resource(s) with missing uploads."
                .format(len(resource_ids_missing_uploads)))


def _initdb():
    drop_tables()
    create_tables()
    print("DB tables are reinitialized")
