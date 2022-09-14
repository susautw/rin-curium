from rin.docutils.flag import ClassNamedFlag, Flag


class NoResponseType(ClassNamedFlag):
    pass


NoResponse = NoResponseType()  #: Represents there is no response returned from a command

Unspecified = Flag("Unspecified")  #: Represents a parameter hasn't specified in a invocation.
