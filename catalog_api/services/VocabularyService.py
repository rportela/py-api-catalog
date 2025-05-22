from catalog_api.models.CatalogModels import CatalogVocabulary
from catalog_api.repository.VocabularyRepository import VocabularyRepository


class VocabularyService:
    """
    Service class for managing CatalogVocabulary operations.
    """

    def __init__(self):
        self.vocabulary_repository = VocabularyRepository()

    def get_by_id(self, vocabulary_id):
        return self.vocabulary_repository.get(vocabulary_id)

    def query(self, query=None):
        return self.vocabulary_repository.query(filter_fn=lambda q: q.filter(query) if query else None)

    def delete_by_id(self, vocabulary_id):
        return self.vocabulary_repository.delete(vocabulary_id)

    def save(self, vocabulary: CatalogVocabulary):
        return self.vocabulary_repository.create(vocabulary)
