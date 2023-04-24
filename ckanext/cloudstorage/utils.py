import os
import click
import unicodecsv as csv
from sqlalchemy import and_ as _and_

from ckan.lib.munge import munge_filename
from ckan import model

from ckanext.cloudstorage.storage import CloudStorage
from ckanext.cloudstorage.model import (
    create_tables,
    drop_tables
)

from ckan.plugins.toolkit import h


def initdb():
    drop_tables()
    create_tables()
    print("DB tables are reinitialized")


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
                        .join(model.Package,
                              model.Resource.package_id == model.Package.id) \
                        .filter(_and_(model.Resource.url_type == u'upload',
                                      model.Resource.state == model.core.State.ACTIVE,
                                      model.Package.state == model.core.State.ACTIVE)) \
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
                                    model.Package.owner_org,
                                    model.Package.state,
                                    model.Resource.state) \
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
                u'upload_size': upload.size / 1000.0,
                u'package_state': resource_fields[6] if resource_fields else None,
                u'resource_state': resource_fields[7] if resource_fields else None})
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
                    u'resource_last_modified',
                    u'package_state',
                    u'resource_state'))
        for upload in uploads:
            w.writerow((
                upload[u'resource_id'],
                upload[u'package_id'],
                upload[u'organization_id'],
                upload[u'resource_filename'],
                upload[u'upload_url'],
                upload[u'upload_size'],
                upload[u'created'],
                upload[u'last_modified'],
                upload[u'package_state'],
                upload[u'resource_state']))
        click.echo(u"Wrote {} row(s) to {}"
                    .format(len(uploads), output_path))


def list_linked_uploads(output_path):
    # type: (str|None) -> None
    used_space, good_uploads = _get_uploads()

    if output_path:
        _write_uploads_to_csv(output_path, good_uploads)
    else:
        used_space, unit = _humanize_space(used_space)
        click.echo(u"Found {} uploads(s) with linked resources. Total space: {} {}."
                    .format(len(good_uploads), used_space, unit))


def list_unlinked_uploads(output_path):
    # type: (str|None) -> None
    used_space, uploads_missing_resources = _get_uploads(get_linked = False)

    if output_path:
        _write_uploads_to_csv(output_path, uploads_missing_resources)
    else:
        used_space, unit = _humanize_space(used_space)
        click.echo(u"Found {} upload(s) with missing or deleted resources. Total space: {} {}."
                    .format(len(uploads_missing_resources), used_space, unit))


def remove_unlinked_uploads():
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


def list_missing_uploads(output_path):
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
                        .join(model.Package,
                              model.Resource.package_id == model.Package.id) \
                        .filter(_and_(model.Resource.url_type == u'upload',
                                      model.Resource.state == model.core.State.ACTIVE,
                                      model.Package.state == model.core.State.ACTIVE)) \
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
                u'upload_size': None,
                u'package_state': model.core.State.ACTIVE,
                u'resource_state': model.core.State.ACTIVE})

    if output_path:
        _write_uploads_to_csv(output_path, resources_missing_uploads)
    else:
        click.echo(u"Found {} resource(s) with missing uploads."
                    .format(len(resources_missing_uploads)))
