class ConfigError(Exception):
    """Used when the configuration to a commands needs to throw an error.
    Includes user inputs: files, flags, args, entities, etc.
    """

    pass


class ResourceError(Exception):
    """A resource failed or is missing."""

    pass
