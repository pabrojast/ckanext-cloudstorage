# -*- coding: utf-8 -*-

import ckan.plugins as p
from ckanext.cloudstorage import views, cli


class MixinPlugin(p.SingletonPlugin):
    p.implements(p.IClick)
    p.implements(p.IBlueprint)

    # IBlueprint

    def get_blueprint(self):
        return [
            views.resource_blueprint
        ]

    # IClick

    def get_commands(self):
        return cli.get_commands()

    