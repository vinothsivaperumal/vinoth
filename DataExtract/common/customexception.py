class InvalidExtract(Exception):
    """Custom exception class."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class InvalidTable(Exception):
    """Custom exception class."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class InvalidFileFormat(Exception):
    """Custom exception class."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)