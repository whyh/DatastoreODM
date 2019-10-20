
class TransactionFailed(Exception):
    def __init__(self, id: str, *args):
        self.id = id
        super().__init__(*args)
