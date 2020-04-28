class ValuesComparableObject:

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __getitem__(self, key):
        return self.__dict__[key]

    def __eq__(self, other):
        """
        Compares this object fields values to another object's fields values.
        """

        return isinstance(other, self.__class__) \
            and self.__dict__ == other.__dict__
