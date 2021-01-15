from eulxml.xmlmap import load_xmlobject_from_string
from bdrxml.rights import Rights
from ..settings import BDR_PUBLIC, BDR_BROWN


class RightsIndexer:

    def __init__(self, rights_bytes):
        self.rights = load_xmlobject_from_string(rights_bytes, Rights)

    def index_data(self):
        rights_index_data = self.rights.index_data()
        rights_index_data.update(self.rights.index_data_hydra())
        rights_index_data.update(
            DisplayIndexer(rights_index_data).index_data()
        )
        return rights_index_data


class DisplayIndexer:

    def __init__(self, rights_index):
        self.rights_index = rights_index

    def index_data(self):
        return {
            '_display_public_bsi': self.public_display,
            '_display_brown_bsi': self.brown_only_display,
            '_display_private_bsi': self.private_display,
        }

    @property
    def rights_display(self):
        return self.rights_index.get('display',[])

    @property
    def is_public_display(self):
        return BDR_PUBLIC in self.rights_display

    @property
    def is_brown_only_display(self):
        return BDR_BROWN in self.rights_display

    @property
    def is_private_display(self):
        return not (self.is_public_display or self.is_brown_only_display)

    @property
    def public_display(self):
        return str(self.is_public_display)

    @property
    def brown_only_display(self):
        return str(self.is_brown_only_display)

    @property
    def private_display(self):
        return str(self.is_private_display)

