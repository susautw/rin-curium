from rin.docutils.flag import ClassNamedFlag, Flag


class NoResponseType(ClassNamedFlag):
    pass


NoResponse = NoResponseType()  #: Represents there is no response returned from a command

NoContextSpecified = Flag("NoContextSpecified")  #: Represents no context specified while registering a command
