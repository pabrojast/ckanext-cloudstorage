#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import os.path
import cgi
import tempfile
import click
import unicodecsv as csv
from sqlalchemy import and_ as _and_

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
from ckan.plugins.toolkit import h

USAGE = """ckanext-cloudstorage

Commands:
    - fix-cors                  Update CORS rules where possible.
    - migrate                   Upload local storage to the remote.
    - initdb                    Reinitalize database tables.
    - list-unlinked-uploads     Lists uploads in the storage container that do not match to any resources.
    - remove-unlinked-uploads   Permanently deletes uploads from the storage container that do not match to any resources.
    - list-missing-uploads      Lists resources IDs that are missing uploads in the storage container.
    - list-uploads              Lists uploads in the storage container that do match to a resource.

Usage:
    cloudstorage fix-cors <domains>... [--c=<config>]
    cloudstorage migrate <path_to_storage> [<resource_id>] [--c=<config>]
    cloudstorage initdb [--c=<config>]
    cloudstorage list-unlinked-uploads [--o=<output>] [--c=<config>]
    cloudstorage remove-unlinked-uploads [--c=<config>]
    cloudstorage list-missing-uploads [--o=<output>] [--c=<config>]
    cloudstorage list-uploads [--o=<output>] [--c=<config>]

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
        elif args['list-uploads']:
            _list_uploads(self.options.output)


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


def _get_uploads(get_linked = True, return_upload_objects_only = False):
    # type: (bool, bool) -> tuple[float, list]
    cs = CloudStorage()

    resource_urls = set(os.path.join(
                        u'resources',
                        id,
                        munge_filename(filename))
                    for id, filename in
                    model.Session.query(
                        model.Resource.id,
                        model.Resource.url) \
                        .filter(model.Resource.url_type == u'upload') \
                        .all())

    uploads = cs.container.list_objects()

    parsed_uploads = []
    total_space_used = 0
    for upload in uploads:
        if (upload.name in resource_urls
            if get_linked else
            upload.name not in resource_urls):

            if return_upload_objects_only:
                parsed_uploads.append(upload)
                continue

            resource_id = upload.name.split('/')[1]
            resource_fields = None
            if resource_id:
                resource_fields = model.Session.query(
                                    model.Resource.id,
                                    model.Resource.url,
                                    model.Resource.package_id,
                                    model.Resource.created,
                                    model.Resource.last_modified,
                                    model.Package.owner_org) \
                                    .join(model.Package, model.Resource.package_id == model.Package.id) \
                                    .filter(_and_(model.Resource.url_type == u'upload',
                                                model.Resource.id == resource_id)) \
                                    .first()
            parsed_uploads.append({
                u'resource_id': resource_fields[0] if resource_fields else None,
                u'resource_filename': resource_fields[1] if resource_fields else None,
                u'package_id': resource_fields[2] if resource_fields else None,
                u'created': h.render_datetime(resource_fields[3]) if resource_fields else None,
                u'last_modified': h.render_datetime(resource_fields[4]) if resource_fields else None,
                u'organization_id': resource_fields[5] if resource_fields else None,
                u'upload_url': upload.name,
                u'upload_size': upload.size / 1000.0})
            total_space_used += upload.size / 1000.0

    return total_space_used, parsed_uploads


def _humanize_space(space):
    # type: (float) -> tuple[float, str]
    parsed_space = space
    for unit in ['KB', 'MB', 'GB', 'TB']:
        if parsed_space < 1000.0:
            return parsed_space, unit
        parsed_space /= 1000.0
    return space, 'KB'


def _write_uploads_to_csv(output_path, uploads):
    #type: (str, list) -> None
    if not uploads:
        click.echo(u"Nothing to write to {}".format(output_path))
        return
    with open(output_path, u'w') as f:
        w = csv.writer(f, encoding='utf-8')
        w.writerow((u'resource_id',
                    u'package_id',
                    u'organization_id',
                    u'resource_filename',
                    u'upload_url',
                    u'upload_file_size_in_kb',
                    u'resource_created',
                    u'resource_last_modified'))
        for upload in uploads:
            w.writerow((
                upload[u'resource_id'],
                upload[u'package_id'],
                upload[u'organization_id'],
                upload[u'resource_filename'],
                upload[u'upload_url'],
                upload[u'upload_size'],
                upload[u'created'],
                upload[u'last_modified']))
        click.echo(u"Wrote {} row(s) to {}"
                    .format(len(uploads), output_path))


def _list_uploads(output_path):
    # type: (str|None) -> None
    used_space, good_uploads = _get_uploads()

    if output_path:
        _write_uploads_to_csv(output_path, good_uploads)
    else:
        used_space, unit = _humanize_space(used_space)
        click.echo(u"Found {} uploads(s) with linked resources. Total space: {} {}."
                    .format(len(good_uploads), used_space, unit))


def _list_unlinked_uploads(output_path):
    # type: (str|None) -> None
    used_space, uploads_missing_resources = _get_uploads(get_linked = False)

    if output_path:
        _write_uploads_to_csv(output_path, uploads_missing_resources)
    else:
        used_space, unit = _humanize_space(used_space)
        click.echo(u"Found {} upload(s) with missing or deleted resources. Total space: {} {}."
                    .format(len(uploads_missing_resources), used_space, unit))


def _remove_unlinked_uploads():
    cs = CloudStorage()

    used_space, uploads_missing_resources = _get_uploads(get_linked = False, return_upload_objects_only = True)

    num_success = 0
    num_failures = 0
    saved_space = 0
    for upload in uploads_missing_resources:
        if cs.container.delete_object(upload):
            click.echo(u"Deleted {}".format(upload.name))
            num_success += 1
            saved_space += upload.size / 1000.0
            used_space -= upload.size / 1000.0
        else:
            click.echo(u"Failed to delete {}".format(upload.name))
            num_failures += 1

    if num_success:
        saved_space, unit = _humanize_space(used_space)
        click.echo(u"Deleted {} upload(s). Saved {} {}."
                    .format(num_success, saved_space, unit))

    if num_failures:
        click.echo(u"Failed to delete {} upload(s).".format(num_failures))

    if used_space:
        used_space, unit = _humanize_space(used_space)
        click.echo(u"Remaining space used by unlinked uploads: {} {}."
                    .format(used_space, unit))


def _list_missing_uploads(output_path):
    # type: (str|None) -> None
    cs = CloudStorage()

    upload_urls = set(u.name for u in cs.container.list_objects())

    resource_fields = model.Session.query(
                        model.Resource.id,
                        model.Resource.url,
                        model.Resource.package_id,
                        model.Resource.created,
                        model.Resource.last_modified,
                        model.Package.owner_org) \
                        .join(model.Package, model.Resource.package_id == model.Package.id) \
                        .filter(model.Resource.url_type == u'upload') \
                        .all()

    resources_missing_uploads = []
    for id, filename, package_id, created, last_modified, organization_id in resource_fields:
        url = os.path.join(
                u'resources',
                id,
                munge_filename(filename))

        if url not in upload_urls:
            resources_missing_uploads.append({
                u'resource_id': id,
                u'resource_filename': filename,
                u'package_id': package_id,
                u'created': h.render_datetime(created),
                u'last_modified': h.render_datetime(last_modified),
                u'organization_id': organization_id,
                u'upload_url': None,
                u'upload_size': None})

    if output_path:
        _write_uploads_to_csv(output_path, resources_missing_uploads)
    else:
        click.echo(u"Found {} resource(s) with missing uploads."
                    .format(len(resources_missing_uploads)))


def _initdb():
    drop_tables()
    create_tables()
    print("DB tables are reinitialized")
