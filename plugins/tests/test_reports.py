class TestClass:
    def test_one(self):
        x = "this"
        assert 'h' in x

    def test_two(self):
        x = "check"
        assert 'h' in x

    value = 0

    def test_one_int(self):
        self.value = 1
        assert self.value == 1

    def test_two_int(self):
        assert self.value == 1
