import click

from ckanext.cloudstorage import utils


def get_commands():
    return [cloudstorage]


@click.group()
def cloudstorage():
    """CloudStorage management commands.
    """
    pass


@cloudstorage.command()
def initdb():
    """Reinitalize database tables."""
    utils.initdb()


@cloudstorage.command()
@click.argument(u'domains')
def fix_cors(domains):
    """Update CORS rules where possible."""
    utils.fix_cors(domains)


@cloudstorage.command()
@click.argument(u'path_to_storage')
@click.argument(u'resource_id', required=False)
def migrate(path_to_storage, resource_id):
    """Upload local storage to the remote."""
    utils.migrate(path_to_storage, resource_id)


@cloudstorage.command()
@click.argument(u'path_to_file')
@click.argument(u'resource_id')
def migrate_file(path_to_file, resource_id):
    """Upload local file to the remote for a given resource."""
    utils.migrate_file(path_to_file, resource_id)


@cloudstorage.command()
@click.option(
    "-o",
    "--output",
    default=None,
    help="The output file path.",
)
def list_unlinked_uploads(output):
    """Lists uploads in the storage container that do not match to any resources."""
    utils.list_linked_uploads(output)


@cloudstorage.command()
def remove_unlinked_uploads():
    """Permanently deletes uploads from the storage container that do not match to any resources."""
    utils.remove_unlinked_uploads()


@cloudstorage.command()
@click.option(
    "-o",
    "--output",
    default=None,
    help="The output file path.",
)
def list_missing_uploads(output):
    """Lists resources that are missing uploads in the storage container."""
    utils.list_missing_uploads(output)


@cloudstorage.command()
@click.option(
    "-o",
    "--output",
    default=None,
    help="The output file path.",
)
def list_linked_uploads(output):
    """Lists uploads in the storage container that do match to a resource."""
    utils.list_linked_uploads(output)


@cloudstorage.command()
@click.option(
    "-r",
    "--resource_id",
    default=None,
    help="A single resource ID to reguess the mimetype for.",
)
@click.option('-v', '--verbose', is_flag=True, default=False, help='Higher verbosity level.')
def reguess_mimetypes(resource_id=None, verbose=False):
    """Reguess mimtypes for all uploads."""
    utils.reguess_mimetypes(resource_id, verbose)


@cloudstorage.command()
@click.option(
    '--older-than',
    default=24,
    type=int,
    help='Only clean up blobs older than this many hours (default: 24).',
)
@click.option(
    '--dry-run',
    is_flag=True,
    default=False,
    help='Show what would be deleted without actually deleting.',
)
@click.option('-v', '--verbose', is_flag=True, default=False, help='Show detailed output.')
def cleanup_temp_blobs(older_than, dry_run, verbose):
    """
    Remove orphan temporary blobs from Azure storage.
    
    This command cleans up blobs in the temp/ directory that were created
    for Azure Direct Upload but were never moved to their final location.
    This can happen if an upload is cancelled or fails.
    """
    utils.cleanup_temp_blobs(older_than, dry_run, verbose)


@cloudstorage.command()
@click.option(
    '--older-than',
    default=24,
    type=int,
    help='Only clean up entries older than this many hours (default: 24).',
)
def cleanup_upload_status(older_than):
    """
    Clean up old entries from the azure_upload_status table.
    
    Removes completed and failed upload status records older than
    the specified number of hours.
    """
    utils.cleanup_upload_status(older_than)
