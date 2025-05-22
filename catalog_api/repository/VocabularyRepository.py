from catalog_api.models.CatalogModels import CatalogVocabulary
from catalog_api.repository.BaseRepository import BaseRepository


class VocabularyRepository(BaseRepository[CatalogVocabulary]):
    """
    Vocabulary repository for managing vocabulary records in the database.
    """

    def __init__(self):
        super().__init__(CatalogVocabulary)
