from catalog_api.models.CatalogModels import CatalogMember
from catalog_api.repository.MemberRepository import MemberRepository


class MemberService:
    """
    Service class for managing CatalogMember operations.
    """

    def __init__(self):
        self.member_repository = MemberRepository()

    def get_by_id(self, member_id):
        return self.member_repository.get(member_id)

    def query(self, query=None):
        return self.member_repository.query(filter_fn=lambda q: q.filter(query) if query else None)

    def delete_by_id(self, member_id):
        return self.member_repository.delete(member_id)

    def create(self, member):
        """
        Create a new Member object.
        """
        return self.member_repository.create(member)

    def update(self, member):
        """
        Update an existing Member object.
        """
        updates = member.to_dict()  # Assuming a to_dict method exists
        return self.member_repository.update(member.id, updates)

    def save(self, member):
        """
        Save a Member object. If the object has an ID, it updates the object; otherwise, it creates a new one.
        """
        if member.id:
            return self.update(member)
        return self.create(member)
