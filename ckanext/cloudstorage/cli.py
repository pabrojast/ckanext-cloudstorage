import click

from ckanext.cloudstorage import utils


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
