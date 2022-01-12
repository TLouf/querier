


class InvalidFilter(Exception):
    """Raised when an operation on a filter is invalid."""
    
    pass

class CredentialsError(Exception):
    """Raised when Connection constructor fails to read or parse the credentials file."""
    
    pass

class AuthentificationError(Exception):
    """Raised by Connection if the credentials are invalid."""
    
    pass

class ServerError(Exception):
    """Raised by Connection when any server-related error takes place during a connection."""
    
    pass

class InternalError(Exception):
    """Raised by when an internally unhandled exception ocurred."""

    pass
